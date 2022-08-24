package org.apache.spark.sql.execution.datasources.parquet;

import org.apache.arrow.lakesoul.io.ArrowCDataWrapper;
import org.apache.arrow.lakesoul.io.read.LakeSoulArrowReader;
import org.apache.arrow.vector.VectorSchemaRoot;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.schema.Type;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.vectorized.ArrowUtils;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;

/**
 * A specialized RecordReader that reads into InternalRows or ColumnarBatches directly using the
 * Parquet column APIs. This is somewhat based on parquet-mr's ColumnReader.
 *
 * TODO: handle complex types, decimal requiring more than 8 bytes, INT96. Schema mismatch.
 * All of these can be handled efficiently and easily with codegen.
 *
 * This class can either return InternalRows or ColumnarBatches. With whole stage codegen
 * enabled, this class returns ColumnarBatches which offers significant performance gains.
 * TODO: make this always return ColumnarBatches.
 */
public class NativeVectorizedReader extends SpecificParquetRecordReaderBase<Object> {
  // The capacity of vectorized batch.
  private int capacity;

  /**
   * Batch of rows that we assemble and the current index we've returned. Every time this
   * batch is used up (batchIdx == numBatched), we populated the batch.
   */
  private int batchIdx = 0;
  private int numBatched = 0;

  /**
   * For each request column, the reader to read this column. This is NULL if this column
   * is missing from the file, in which case we populate the attribute with NULL.
   */
  private VectorizedColumnReader[] columnReaders;

  /**
   * The number of rows that have been returned.
   */
  private long rowsReturned;

  /**
   * The number of rows that have been reading, including the current in flight row group.
   */
  private long totalCountLoadedSoFar = 0;

  /**
   * For each column, true if the column is missing in the file and we'll instead return NULLs.
   */
  private boolean[] missingColumns;

  /**
   * The timezone that timestamp INT96 values should be converted to. Null if no conversion. Here to
   * workaround incompatibilities between different engines when writing timestamp values.
   */
  private final ZoneId convertTz;

  /**
   * The mode of rebasing date/timestamp from Julian to Proleptic Gregorian calendar.
   */
  private final String datetimeRebaseMode;

  /**
   * The mode of rebasing INT96 timestamp from Julian to Proleptic Gregorian calendar.
   */
  private final String int96RebaseMode;

  /**
   * columnBatch object that is used for batch decoding. This is created on first use and triggers
   * batched decoding. It is not valid to interleave calls to the batched interface with the row
   * by row RecordReader APIs.
   * This is only enabled with additional flags for development. This is still a work in progress
   * and currently unsupported cases will fail with potentially difficult to diagnose errors.
   * This should be only turned on for development to work on this feature.
   *
   * When this is set, the code will branch early on in the RecordReader APIs. There is no shared
   * code between the path that uses the MR decoders and the vectorized ones.
   *
   * TODOs:
   *  - Implement v2 page formats (just make sure we create the correct decoders).
   */
  private ColumnarBatch columnarBatch;

  private WritableColumnVector[] columnVectors;

  /**
   * If true, this class returns batches instead of rows.
   */
  private boolean returnColumnarBatch;

  /**
   * The memory mode of the columnarBatch
   */
  private final MemoryMode MEMORY_MODE;

  public NativeVectorizedReader(
          ZoneId convertTz,
          String datetimeRebaseMode,
          String int96RebaseMode,
          boolean useOffHeap,
          int capacity,
          PartitionedFile file) {
    this.convertTz = convertTz;
    this.datetimeRebaseMode = datetimeRebaseMode;
    this.int96RebaseMode = int96RebaseMode;
    MEMORY_MODE = useOffHeap ? MemoryMode.OFF_HEAP : MemoryMode.ON_HEAP;
    System.out.println("[Debug][huazeng]on NativeVectorizedReader, capacity:"+capacity);
    this.capacity = capacity;
    this.file = file;
  }


  /**
   * Implementation of RecordReader API.
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
          throws IOException, InterruptedException, UnsupportedOperationException {
    super.initialize(inputSplit, taskAttemptContext);
    initializeInternal();
  }

  /**
   * Utility API that will read all the data in path. This circumvents the need to create Hadoop
   * objects to use this class. `columns` can contain the list of columns to project.
   */
  @Override
  public void initialize(String path, List<String> columns) throws IOException,
          UnsupportedOperationException {
    super.initialize(path, columns);
    initializeInternal();
  }

  @Override
  public void close() throws IOException {
    if (columnarBatch != null) {
      columnarBatch.close();
      columnarBatch = null;
    }
    super.close();
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    resultBatch();

    if (returnColumnarBatch) return nextBatch();

    if (batchIdx >= numBatched) {
      if (!nextBatch()) return false;
    }
    ++batchIdx;
    return true;
  }

  @Override
  public Object getCurrentValue() {
    System.out.println("[Debug][huazeng]on getCurrentValue, rowsReturned:"+rowsReturned + " totalRowCount:" + totalRowCount);
    System.out.println("[Debug][huazeng]on getCurrentValue, columnarBatch.numCols():"+columnarBatch.numCols() + "columnarBatch.numRows():"+columnarBatch.numRows());
    if (returnColumnarBatch) return columnarBatch;
    return columnarBatch.getRow(batchIdx - 1);
//    if (returnColumnarBatch) return columnarBatch;
//    return columnarBatch.getRow(batchIdx - 1);
  }

  @Override
  public float getProgress() {
    return (float) rowsReturned / totalRowCount;
  }

  // Creates a columnar batch that includes the schema from the data files and the additional
  // partition columns appended to the end of the batch.
  // For example, if the data contains two columns, with 2 partition columns:
  // Columns 0,1: data columns
  // Column 2: partitionValues[0]
  // Column 3: partitionValues[1]
  private void initBatch(
          MemoryMode memMode,
          StructType partitionColumns,
          InternalRow partitionValues) throws IOException {
    StructType partitionSchema = new StructType();
    if (partitionColumns != null) {
      System.out.println("[Debug][huazeng]on initBatch, partitionValues:"+partitionValues.toString());
      for (StructField f : partitionColumns.fields()) {
        partitionSchema = partitionSchema.add(f);
      }
    }
    if (memMode == MemoryMode.OFF_HEAP) {
      partitionColumnVectors = OffHeapColumnVector.allocateColumns(capacity, partitionSchema);
    } else {
      partitionColumnVectors = OnHeapColumnVector.allocateColumns(capacity, partitionSchema);
    }
    if (partitionColumns != null) {
      for (int i = 0; i < partitionColumns.fields().length; i++) {
        System.out.println("[Debug][huazeng]on initBatch: partitionColumnVectors.length=" + partitionColumnVectors.length);
        ColumnVectorUtils.populate(partitionColumnVectors[i], partitionValues, i);
        partitionColumnVectors[i].setIsConstant();
      }
    }
    if (nativeReader.hasNext()) {
      VectorSchemaRoot vsr = nativeReader.nextResultVectorSchemaRoot();
      columnarBatch = new ColumnarBatch(concatBatchVectorWithPartitionVectors(ArrowUtils.asArrayColumnVector(vsr)), vsr.getRowCount());
    } else {
      throw new IOException("expecting more recordbatch but reached last block. Read "
              + rowsReturned + " out of " + totalRowCount);
    }
  }

  private void initBatch() throws IOException {
    initBatch(MEMORY_MODE, null, null);
  }

  public void initBatch(StructType partitionColumns, InternalRow partitionValues) throws IOException {
    initBatch(MEMORY_MODE, partitionColumns, partitionValues);
  }

  /**
   * Returns the ColumnarBatch object that will be used for all rows returned by this reader.
   * This object is reused. Calling this enables the vectorized reader. This should be called
   * before any calls to nextKeyValue/nextBatch.
   */
  public ColumnarBatch resultBatch() throws IOException {
    if (columnarBatch == null) initBatch();
    return columnarBatch;
  }

  /**
   * Can be called before any rows are returned to enable returning columnar batches directly.
   */
  public void enableReturningBatches() {
    returnColumnarBatch = true;
  }

  /**
   * Advances to the next batch of rows. Returns false if there are no more.
   */
  public boolean nextBatch() throws IOException {
    columnarBatch.setNumRows(0);
    if (rowsReturned >= totalRowCount) return false;
//    checkEndOfRowGroup();
    int num = (int) Math.min((long) capacity, totalRowCount - rowsReturned);
    rowsReturned += num;
    columnarBatch.setNumRows(num);
    numBatched = num;
    batchIdx = 0;
    return true;
//    for (WritableColumnVector vector : columnVectors) {
//      vector.reset();
//    }
//    columnarBatch.setNumRows(0);
//    if (rowsReturned >= totalRowCount) return false;
//    checkEndOfRowGroup();
//
//    int num = (int) Math.min((long) capacity, totalCountLoadedSoFar - rowsReturned);
//    for (int i = 0; i < columnReaders.length; ++i) {
//      if (columnReaders[i] == null) continue;
//      columnReaders[i].readBatch(num, columnVectors[i]);
//    }
//    rowsReturned += num;
//    columnarBatch.setNumRows(num);
//    numBatched = num;
//    batchIdx = 0;
//    return true;
  }

  private ColumnVector[] concatBatchVectorWithPartitionVectors(ColumnVector[] batchVectors){
    ColumnVector[] descColumnVectors = new ColumnVector[batchVectors.length + partitionColumnVectors.length];
    System.arraycopy(batchVectors, 0, descColumnVectors, 0, batchVectors.length);
    System.arraycopy(partitionColumnVectors, 0, descColumnVectors, batchVectors.length,  partitionColumnVectors.length);
    System.out.println("[Debug][huazeng]on concatBatchVectorWithPartitionVectors");
//    System.out.println(descColumnVectors[batchVectors.length]);
    return descColumnVectors;
  }

  private void initializeInternal() throws IOException, UnsupportedOperationException {
    // Check that the requested schema is supported.
    missingColumns = new boolean[requestedSchema.getFieldCount()];
    List<ColumnDescriptor> columns = requestedSchema.getColumns();
    List<String[]> paths = requestedSchema.getPaths();
    for (int i = 0; i < requestedSchema.getFieldCount(); ++i) {
      Type t = requestedSchema.getFields().get(i);
      if (!t.isPrimitive() || t.isRepetition(Type.Repetition.REPEATED)) {
        throw new UnsupportedOperationException("Complex types not supported.");
      }

      String[] colPath = paths.get(i);
      if (fileSchema.containsPath(colPath)) {
        ColumnDescriptor fd = fileSchema.getColumnDescription(colPath);
        if (!fd.equals(columns.get(i))) {
          throw new UnsupportedOperationException("Schema evolution not supported.");
        }
        missingColumns[i] = false;
      } else {
        if (columns.get(i).getMaxDefinitionLevel() == 0) {
          // Column is missing in data but the required data is non-nullable. This file is invalid.
          throw new IOException("Required column is missing in data file. Col: " +
                  Arrays.toString(colPath));
        }
        missingColumns[i] = true;
      }
    }
    // initalizing native reader
    wrapper = new ArrowCDataWrapper();
    wrapper.initializeConfigBuilder();
    wrapper.addFile(file.filePath());
    // Initialize missing columns with nulls.
    for (int i = 0; i < missingColumns.length; i++) {
      if (!missingColumns[i]) {
        wrapper.addColumn(sparkSchema.fields()[i].name());
      }
    }

    wrapper.setThreadNum(2);
    wrapper.createReader();
    wrapper.startReader(bool -> {});
    nativeReader = new LakeSoulArrowReader(wrapper);
  }

  private void checkEndOfRowGroup() throws IOException {

    if (rowsReturned != totalCountLoadedSoFar) return;
    PageReadStore pages = reader.readNextRowGroup();
    if (pages == null) {
      throw new IOException("expecting more rows but reached last block. Read "
              + rowsReturned + " out of " + totalRowCount);
    }
//    List<ColumnDescriptor> columns = requestedSchema.getColumns();
//    List<Type> types = requestedSchema.asGroupType().getFields();
//    columnReaders = new VectorizedColumnReader[columns.size()];
//    for (int i = 0; i < columns.size(); ++i) {
//      if (missingColumns[i]) continue;
//      columnReaders[i] = new VectorizedColumnReader(
//              columns.get(i),
//              types.get(i).getOriginalType(),
//              pages.getPageReader(columns.get(i)),
//              convertTz,
//              datetimeRebaseMode,
//              int96RebaseMode);
//    }
    totalCountLoadedSoFar += pages.getRowCount();
  }

  private ArrowCDataWrapper wrapper;
  private LakeSoulArrowReader nativeReader;
  private WritableColumnVector[] partitionColumnVectors;

  private PartitionedFile file;
}


//public class NativeVectorizedReader implements AutoCloseable {
//
//  // The capacity of vectorized batch.
//  private int capacity;
//  private ArrowCDataWrapper wrapper;
//  private LakeSoulArrowReader reader;
//
//  private WritableColumnVector[] partitionColumnVectors;
//
//  /**
//   * For each column, true if the column is missing in the file and we'll instead return NULLs.
//   */
//  private boolean[] missingColumns;
//
//  /**
//   * The memory mode of the columnarBatch
//   */
//  private final MemoryMode MEMORY_MODE;
//
//  public NativeVectorizedReader(PartitionedFile file, StructType partitionSchema, int capacity){
//    wrapper=new ArrowCDataWrapper();
//    wrapper.initializeConfigBuilder();
//    wrapper.addFile(file.filePath());
//
//    wrapper.setThreadNum(2);
//    wrapper.createReader();
//    wrapper.startReader(bool -> {});
//    reader = new LakeSoulArrowReader(wrapper);
//
//    MEMORY_MODE = MemoryMode.ON_HEAP;
//    this.capacity = capacity;
//  }
//
//  public boolean nextKeyValue() throws IOException {
//    return reader.hasNext();
//  }
//
//  public ColumnarBatch getCurrentValue() {
//    VectorSchemaRoot vsr = reader.nextResultVectorSchemaRoot();
//    return new ColumnarBatch(concatBatchVectorWithPartitionVectors(ArrowUtils.asArrayColumnVector(vsr)), capacity);
//  }
//
//  private void initializeInternal() throws IOException, UnsupportedOperationException {
//    ParquetMetadata footer = readFooter(config, file, range(0, length));
//
//    List<BlockMetaData> blocks = footer.getBlocks();
//    this.fileSchema = footer.getFileMetaData().getSchema();
//    // Check that the requested schema is supported.
//    missingColumns = new boolean[requestedSchema.getFieldCount()];
//    List<ColumnDescriptor> columns = requestedSchema.getColumns();
//    List<String[]> paths = requestedSchema.getPaths();
//    for (int i = 0; i < requestedSchema.getFieldCount(); ++i) {
//      Type t = requestedSchema.getFields().get(i);
//      if (!t.isPrimitive() || t.isRepetition(Type.Repetition.REPEATED)) {
//        throw new UnsupportedOperationException("Complex types not supported.");
//      }
//
//      String[] colPath = paths.get(i);
//      if (fileSchema.containsPath(colPath)) {
//        ColumnDescriptor fd = fileSchema.getColumnDescription(colPath);
//        if (!fd.equals(columns.get(i))) {
//          throw new UnsupportedOperationException("Schema evolution not supported.");
//        }
//        missingColumns[i] = false;
//      } else {
//        if (columns.get(i).getMaxDefinitionLevel() == 0) {
//          // Column is missing in data but the required data is non-nullable. This file is invalid.
//          throw new IOException("Required column is missing in data file. Col: " +
//                  Arrays.toString(colPath));
//        }
//        missingColumns[i] = true;
//      }
//    }
//  }
//
//  // partition columns appended to the end of the batch.
//  // For example, if the data contains two columns, with 2 partition columns:
//  // Columns 0,1: data columns
//  // Column 2: partitionValues[0]
//  // Column 3: partitionValues[1]
//  public void initializePartitionColumns(
//          MemoryMode memMode,
//          StructType partitionColumns,
//          InternalRow partitionValues) {
//    System.out.println("[Debug][huazeng]on initializePartitionColumns");
//    System.out.println("[Debug][huazeng]on initializePartitionColumns, partitionValues:"+partitionValues.toString());
//    StructType partitionSchema = new StructType();
//    if (partitionColumns != null) {
//      for (StructField f : partitionColumns.fields()) {
//        partitionSchema = partitionSchema.add(f);
//      }
//    }
//    if (memMode == MemoryMode.OFF_HEAP) {
//      partitionColumnVectors = OffHeapColumnVector.allocateColumns(capacity, partitionSchema);
//    } else {
//      partitionColumnVectors = OnHeapColumnVector.allocateColumns(capacity, partitionSchema);
//    }
//    if (partitionColumns != null) {
//      for (int i = 0; i < partitionColumns.fields().length; i++) {
//        ColumnVectorUtils.populate(partitionColumnVectors[i], partitionValues, 0);
//        partitionColumnVectors[i].setIsConstant();
//      }
//    }
//    System.out.println("[Debug][huazeng]on initializePartitionColumns: partitionColumnVectors.length=" + partitionColumnVectors.length);
////    System.out.println(partitionColumnVectors[0]);
//  }
//
//  private ColumnVector[] concatBatchVectorWithPartitionVectors(ColumnVector[] batchVectors){
//    ColumnVector[] descColumnVectors = new ColumnVector[batchVectors.length + partitionColumnVectors.length];
//    System.arraycopy(batchVectors, 0, descColumnVectors, 0, batchVectors.length);
//    System.arraycopy(partitionColumnVectors, 0, descColumnVectors, batchVectors.length,  partitionColumnVectors.length);
//    System.out.println("[Debug][huazeng]on concatBatchVectorWithPartitionVectors");
////    System.out.println(descColumnVectors[batchVectors.length]);
//    return descColumnVectors;
//  }
//
//  private void initBatch() {
//    initBatch(MEMORY_MODE, null, null);
//  }
//
//  public void initBatch(StructType partitionColumns, InternalRow partitionValues) {
//    initBatch(MEMORY_MODE, partitionColumns, partitionValues);
//  }
//
//
//  /**
//   * Closes this resource, relinquishing any underlying resources.
//   * This method is invoked automatically on objects managed by the
//   * {@code try}-with-resources statement.
//   *
//   * @throws Exception if this resource cannot be closed
//   * @apiNote While this interface method is declared to throw {@code
//   * Exception}, implementers are <em>strongly</em> encouraged to
//   * declare concrete implementations of the {@code close} method to
//   * throw more specific exceptions, or to throw no exception at all
//   * if the close operation cannot fail.
//   *
//   * <p> Cases where the close operation may fail require careful
//   * attention by implementers. It is strongly advised to relinquish
//   * the underlying resources and to internally <em>mark</em> the
//   * resource as closed, prior to throwing the exception. The {@code
//   * close} method is unlikely to be invoked more than once and so
//   * this ensures that the resources are released in a timely manner.
//   * Furthermore it reduces problems that could arise when the resource
//   * wraps, or is wrapped, by another resource.
//   *
//   * <p><em>Implementers of this interface are also strongly advised
//   * to not have the {@code close} method throw {@link
//   * InterruptedException}.</em>
//   * <p>
//   * This exception interacts with a thread's interrupted status,
//   * and runtime misbehavior is likely to occur if an {@code
//   * InterruptedException} is {@linkplain Throwable#addSuppressed
//   * suppressed}.
//   * <p>
//   * More generally, if it would cause problems for an
//   * exception to be suppressed, the {@code AutoCloseable.close}
//   * method should not throw it.
//   *
//   * <p>Note that unlike the {@link Closeable#close close}
//   * method of {@link Closeable}, this {@code close} method
//   * is <em>not</em> required to be idempotent.  In other words,
//   * calling this {@code close} method more than once may have some
//   * visible side effect, unlike {@code Closeable.close} which is
//   * required to have no effect if called more than once.
//   * <p>
//   * However, implementers of this interface are strongly encouraged
//   * to make their {@code close} methods idempotent.
//   */
//  @Override
//  public void close() throws Exception {
//
//  }
//}
