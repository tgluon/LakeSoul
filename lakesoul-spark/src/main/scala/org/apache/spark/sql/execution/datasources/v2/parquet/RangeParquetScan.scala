package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.lakesoul.{LakeSoulFileIndexV2, LakeSoulTableForCdc}
import org.apache.spark.sql.lakesoul.utils.TableInfo
import org.apache.spark.sql.sources.{EqualTo, Filter, Not}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import java.util.OptionalLong

case class RangeParquetScan(sparkSession: SparkSession,
                              hadoopConf: Configuration,
                              fileIndex: LakeSoulFileIndexV2,
                              dataSchema: StructType,
                              readDataSchema: StructType,
                              readPartitionSchema: StructType,
                              pushedFilters: Array[Filter],
                              options: CaseInsensitiveStringMap,
                              tableInfo: TableInfo,
                              partitionFilters: Seq[Expression] = Seq.empty
                              )
  extends Scan with Batch
    with SupportsReportStatistics with Logging {

  override def readSchema(): StructType =
    StructType(readDataSchema.fields ++ readPartitionSchema.fields)

  override def createReaderFactory(): PartitionReaderFactory = {
    logInfo("[Debug][huazeng]createReaderFactory")
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))


    RangeParquetPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, pushedFilters)
  }

  override def planInputPartitions(): Array[InputPartition] = ???

  override def estimateStatistics(): Statistics = {
    new Statistics {
      override def sizeInBytes(): OptionalLong = {
        val compressionFactor = sparkSession.sessionState.conf.fileCompressionFactor
        val size = (compressionFactor * fileIndex.sizeInBytes).toLong
        OptionalLong.of(size)
      }

      override def numRows(): OptionalLong = OptionalLong.empty()
    }
  }
}
