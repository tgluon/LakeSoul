package org.apache.spark.sql.execution.datasources.v2.merge.parquet.Native

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.v2.merge.MergePartitionedFile
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.MergeLogic
import org.apache.spark.sql.vectorized.ColumnarBatch

class NativeMergeLogic(filesInfo: (Array[MergePartitionedFile], PartitionReader[ColumnarBatch])) extends MergeLogic {
  var rowId: Int = -1

  var columnarBatch: ColumnarBatch = _

  def alive(): Boolean = true

  def getRow: InternalRow = {
    columnarBatch.getRow(rowId)
  }

  /** The file readers have been read should be closed at once. */
  override def closeReadFileReader(): Unit = ???

}
