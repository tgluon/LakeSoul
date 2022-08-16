package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.merge.{MergeFilePartition, MergePartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.parquet.Native.NativeFilePartitionReader
import org.apache.spark.sql.vectorized.ColumnarBatch


abstract class RangeFilePartitionReaderFactory extends PartitionReaderFactory with Logging{
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    logInfo("[Debug][huazeng]on org.apache.spark.sql.execution.datasources.v2.parquet.RangeFilePartitionReaderFactory.createReader")
    assert(partition.isInstanceOf[MergeFilePartition])
    val filePartition = partition.asInstanceOf[MergeFilePartition]

    val iter = filePartition.files.toIterator.map { files =>
      assert(files.forall(_.isInstanceOf[MergePartitionedFile]))
      files -> buildColumnarReader(files)
    }.toSeq
    new NativeFilePartitionReader[InternalRow](iter)
  }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {

    throw new Exception("this function is not supported")
  }

  def buildColumnarReader(partitionedFile: Array[MergePartitionedFile]): PartitionReader[ColumnarBatch] = {
    throw new UnsupportedOperationException("Cannot create columnar reader.")
  }
}
