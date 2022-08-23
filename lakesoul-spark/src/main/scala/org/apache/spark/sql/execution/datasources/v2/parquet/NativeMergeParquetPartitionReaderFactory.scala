package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.v2.merge.MergePartitionedFile
import org.apache.spark.sql.execution.datasources.v2.parquet.Native.NativeMergeVectorizedReader
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration


/**
  * A factory used to create Parquet readers.
  *
  * @param sqlConf         SQL configuration.
  * @param broadcastedConf Broadcast serializable Hadoop Configuration.
  * @param dataSchema      Schema of Parquet files.
  * @param readDataSchema  Required schema of Parquet files.
  * @param partitionSchema Schema of partitions.
  *                        //  * @param filterMap Filters to be pushed down in the batch scan.
  */
case class NativeMergeParquetPartitionReaderFactory(sqlConf: SQLConf,
                                                    broadcastedConf: Broadcast[SerializableConfiguration],
                                                    dataSchema: StructType,
                                                    readDataSchema: StructType,
                                                    partitionSchema: StructType,
                                                    filters: Array[Filter])
  extends NativeMergeFilePartitionReaderFactory with Logging{

  private val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled
  private val capacity = sqlConf.parquetVectorizedReaderBatchSize


  def createNativeVectorizedReader(files: Array[MergePartitionedFile]): NativeMergeVectorizedReader={
    logInfo("[Debug][huazeng]on createNativeVectorizedReader, partitionSchema:" + partitionSchema)
    val vectorizedReader = new NativeMergeVectorizedReader(files, partitionSchema, 482)
    vectorizedReader
  }

  override def buildColumnarReader(files: Array[MergePartitionedFile]): PartitionReader[ColumnarBatch] = {
    logInfo("[Debug][huazeng]on buildColumnarReader")
    val vectorizedReader = createNativeVectorizedReader(files)

    new PartitionReader[ColumnarBatch] {
      override def next(): Boolean = vectorizedReader.nextKeyValue()

      override def get(): ColumnarBatch =
        vectorizedReader.getCurrentValue

      override def close(): Unit = vectorizedReader.close()
    }
  }


}
