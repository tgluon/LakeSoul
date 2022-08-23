package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.v2.merge.{MergeFilePartition, MergePartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.parquet.Native.NativeFilePartitionReader
import org.apache.spark.sql.vectorized.ColumnarBatch


abstract class NativeMergeFilePartitionReaderFactory extends PartitionReaderFactory with Logging{
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    logInfo("[Debug][huazeng]on createReader " + partition.toString)
    assert(partition.isInstanceOf[MergeFilePartition])
    val filePartition = partition.asInstanceOf[MergeFilePartition]

    val iter = filePartition.files.toIterator.map { files =>
//      assert(files.forall(_.isInstanceOf[MergePartitionedFile]))
      logInfo("[Debug][huazeng]on createReader "+files.forall(_.isInstanceOf[MergePartitionedFile]).toString)
      val reader = buildColumnarReader(files)
      files -> reader
    }.toSeq
    logInfo("[Debug][huazeng]on createReader " + iter.toString())
    val reader = new NativeFilePartitionReader[InternalRow](iter)
    reader
  }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {

    throw new Exception("this function is not supported")
  }

  def buildColumnarReader(partitionedFile: Array[MergePartitionedFile]): PartitionReader[ColumnarBatch] = {
    logInfo("[Debug][huazeng]on buildColumnarReader " + partitionedFile.toString)
    throw new UnsupportedOperationException("Cannot create columnar reader.")
  }
}
