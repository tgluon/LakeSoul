package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

case class NativeParquetScan(override val sparkSession: SparkSession,
                             override val hadoopConf: Configuration,
                             override val fileIndex: PartitioningAwareFileIndex,
                             override val dataSchema: StructType,
                             override val readDataSchema: StructType,
                             override val readPartitionSchema: StructType,
                             override val pushedFilters: Array[Filter],
                             override val options: CaseInsensitiveStringMap,
                             override val partitionFilters: Seq[Expression] = Seq.empty,
                             override val dataFilters: Seq[Expression] = Seq.empty)
  extends ParquetScan(sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema, readPartitionSchema, pushedFilters, options, partitionFilters, dataFilters) {
  override def createReaderFactory(): PartitionReaderFactory = {
    logInfo("[Debug][huazeng]on createReaderFactory")
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))


    NativeParquetPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, pushedFilters)
  }
}