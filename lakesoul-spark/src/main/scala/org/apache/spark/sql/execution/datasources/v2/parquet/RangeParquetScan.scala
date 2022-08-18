package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.merge.{MergeDeltaParquetScan, MergeFilePartition, MergePartitionedFile}
import org.apache.spark.sql.internal.SQLConf
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
                              partitionFilters: Seq[Expression] = Seq.empty,
                              dataFilters: Seq[Expression] = Seq.empty
                              )
  extends MergeDeltaParquetScan(sparkSession,
    hadoopConf,
    fileIndex,
    dataSchema,
    readDataSchema,
    readPartitionSchema,
    pushedFilters,
    options,
    tableInfo,
    partitionFilters,
    dataFilters) {


  override def createReaderFactory(): PartitionReaderFactory = {
    logInfo("[Debug][huazeng]createReaderFactory")
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))


    RangeParquetPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, pushedFilters)
  }

  override def getFilePartitions(conf: SQLConf,
                                 partitionedFiles: Seq[MergePartitionedFile],
                                 bucketNum: Int): Seq[MergeFilePartition] = {
    val groupByPartition = partitionedFiles.groupBy(_.rangeKey)

    assert(groupByPartition.size == 1)

    val fileWithBucketId = groupByPartition.head._2
      .groupBy(_.fileBucketId).map(f => (f._1, f._2.toArray))

    Seq.tabulate(bucketNum) { bucketId =>
      var files = fileWithBucketId.getOrElse(bucketId, Array.empty)
      val isSingleFile = files.size == 1

      if(!isSingleFile){
        val versionFiles=for(version <- 0 to files.size-1) yield files(version).copy(writeVersion = version + 1)
        files=versionFiles.toArray
      }
      MergeFilePartition(bucketId, Array(files), isSingleFile)
    }
  }
}
