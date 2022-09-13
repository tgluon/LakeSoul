/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.tables.LakeSoulTable

import java.util.Locale
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.lakesoul.test.LakeSoulSQLCommandTest
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}
import org.apache.spark.sql.{AnalysisException, QueryTest}

import scala.util.control.NonFatal


class NotSupportedDDLSuite
  extends NotSupportedDDLBase
    with SharedSparkSession
    with LakeSoulSQLCommandTest


abstract class NotSupportedDDLBase extends QueryTest
  with SQLTestUtils {

  val format = "lakesoul"

  val nonPartitionedTableName = "lakeSoulTbl"

  val partitionedTableName = "partitionedLakeSoulTbl"

  protected override def beforeEach(): Unit = {
    super.beforeAll()
    try {
      sql(
        s"""
           |CREATE TABLE $nonPartitionedTableName
           |USING $format
           |AS SELECT 1 as a, 'a' as b
         """.stripMargin)

      sql(
        s"""
           |CREATE TABLE $partitionedTableName (a INT, b STRING, p1 INT)
           |USING $format
           |PARTITIONED BY (p1)
          """.stripMargin)
      sql(s"INSERT INTO $partitionedTableName SELECT 1, 'A', 2")
    } catch {
      case NonFatal(e) =>
        afterAll()
        throw e
    }
  }

  protected override def afterEach(): Unit = {
    try {
      val location = Seq(nonPartitionedTableName, partitionedTableName).map(tbl => {
        try {
          Option(spark.sessionState.catalog.getTableMetadata(TableIdentifier(tbl)).location)
        } catch {
          case e: Exception => None
        }
      })

      sql(s"DROP TABLE IF EXISTS $nonPartitionedTableName")
      sql(s"DROP TABLE IF EXISTS $partitionedTableName")
      location.foreach(loc => {
        if (loc.isDefined) {
          try {
            LakeSoulTable.forPath(loc.get.toString).dropTable()
          } catch {
            case e: Exception =>
          }
        }
      })

    } finally {
      super.afterAll()
    }
  }

  private def assertUnsupported(query: String, messages: String*): Unit = {
    val allErrMessages = "operation not allowed" +: "does not support" +: "is not supported" +: messages
    val e = intercept[AnalysisException] {
      sql(query)
    }
    println(e.getMessage().toLowerCase(Locale.ROOT))
    assert(allErrMessages.exists(err => e.getMessage.toLowerCase(Locale.ROOT).contains(err)))
  }

  private def assertIgnored(query: String): Unit = {
    val outputStream = new java.io.ByteArrayOutputStream()
    Console.withOut(outputStream) {
      sql(query)
    }
    assert(outputStream.toString.contains("The request is ignored"))
  }

  test("bucketing is not supported for lakesoul tables") {
    withTable("tbl") {
      assertUnsupported(
        s"""
           |CREATE TABLE tbl(a INT, b INT)
           |USING $format
           |CLUSTERED BY (a) INTO 5 BUCKETS
        """.stripMargin)
    }
  }

  test("CREATE TABLE LIKE") {
    withTable("tbl") {
      assertUnsupported(s"CREATE TABLE tbl LIKE $nonPartitionedTableName")
    }
  }

  test("ANALYZE TABLE PARTITION") {
    assertUnsupported(s"ANALYZE TABLE $partitionedTableName PARTITION (p1) COMPUTE STATISTICS",
      "analyze table is not supported for v2 tables")
  }

  test("ALTER TABLE ADD PARTITION") {
    assertUnsupported(s"ALTER TABLE $partitionedTableName ADD PARTITION (p1=3)",
      "can not alter partitions")
  }

  test("ALTER TABLE DROP PARTITION") {
    assertUnsupported(s"ALTER TABLE $partitionedTableName DROP PARTITION (p1=2)",
      "can not alter partitions")
  }

  test("ALTER TABLE RECOVER PARTITIONS") {
    assertUnsupported(s"ALTER TABLE $partitionedTableName RECOVER PARTITIONS")
    assertUnsupported(s"MSCK REPAIR TABLE $partitionedTableName")
  }

  test("ALTER TABLE SET SERDEPROPERTIES") {
    assertUnsupported(s"ALTER TABLE $nonPartitionedTableName SET SERDEPROPERTIES (s1=3)")
  }

  test("ALTER TABLE RENAME TO") {
    assertUnsupported(s"ALTER TABLE $nonPartitionedTableName RENAME TO newTbl")
  }


  test("LOAD DATA") {
    assertUnsupported(
      s"""LOAD DATA LOCAL INPATH '/path/to/home' INTO TABLE $nonPartitionedTableName""",
      "load data is not supported for v2 tables")
  }

  test("INSERT OVERWRITE DIRECTORY") {
    assertUnsupported(s"INSERT OVERWRITE DIRECTORY '/path/to/home' USING $format VALUES (1, 'a')")
  }
}
