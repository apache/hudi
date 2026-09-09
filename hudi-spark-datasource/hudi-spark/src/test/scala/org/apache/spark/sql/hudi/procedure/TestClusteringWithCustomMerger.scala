/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hudi.procedure

import org.apache.hudi.{DefaultSparkRecordMerger, HoodieDataSourceHelpers}
import org.apache.hudi.common.config.HoodieReaderConfig
import org.apache.hudi.common.model.HoodieRecordMerger
import org.apache.hudi.common.table.timeline.HoodieTimeline

import org.apache.hadoop.fs.Path

import scala.collection.JavaConverters._

/**
 * Regression test for <a href="https://github.com/apache/hudi/issues/18980">HUDI issue #18980</a>:
 * clustering on a table configured with {@code hoodie.write.record.merge.mode=CUSTOM} and a custom
 * Spark merger failed with
 * "No valid spark merger implementation set for `hoodie.write.record.merge.custom.implementation.classes`".
 *
 * <p>The merge mode and strategy id are persisted as table config, but the custom merger impl classes
 * are a write-side config that is not persisted. Before the fix,
 * {@code ClusteringExecutionStrategy.getReaderProperties} built a fresh property set containing only
 * the spill/memory keys, dropping the impl classes, so the file group reader could not resolve the
 * configured merger. The fix seeds the reader properties from the full write config.
 *
 * <p>Both clustering execution paths call {@code getReaderProperties} and reproduce the bug:
 * the row-writer path ({@code MultipleSparkJobExecutionStrategy#readRecordsForGroupAsRow}, used when
 * {@code hoodie.datasource.write.row.writer.enable} is true — the default the reporter hit) and the
 * RDD path ({@code MultipleSparkJobExecutionStrategy#readRecordsForGroup} when it is false). The test
 * is parameterized over both.
 *
 * <p>This lives in {@code hudi-spark} (not {@code hudi-spark-client}): a CUSTOM/SPARK-typed merger
 * forces the InternalRow write path, which needs a concrete {@code SparkXXXAdapter} and real Spark
 * records — neither is available to {@code hudi-spark-client}'s test classpath / Avro test-data path.
 */
class TestClusteringWithCustomMerger extends HoodieSparkProcedureTestBase {

  test("Test clustering with CUSTOM record merge mode and a custom Spark merger") {
    // hoodie.datasource.write.row.writer.enable: true exercises the row-writer clustering path
    // (the default the reporter hit), false exercises the RDD path. Both call getReaderProperties.
    Seq("true", "false").foreach { rowWriterEnabled =>
      withTempDir { tmp =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        val mergerClass = classOf[CustomSparkRecordMergerForClustering].getName

        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  part long
             |) using hudi
             | tblproperties (
             |  primaryKey = 'id',
             |  type = 'mor',
             |  orderingFields = 'ts',
             |  "hoodie.write.record.merge.mode" = "CUSTOM",
             |  "hoodie.write.record.merge.strategy.id" = "${HoodieRecordMerger.CUSTOM_MERGE_STRATEGY_UUID}",
             |  "${HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY}" = "$mergerClass",
             |  "hoodie.parquet.small.file.limit" = "0"
             | )
             | partitioned by (part)
             | location '$basePath'
       """.stripMargin)

        withSQLConf(
          "hoodie.compact.inline" -> "false",
          "hoodie.compact.schedule.inline" -> "false") {
          // Several commits into the same partition create multiple file groups for clustering to
          // combine (small.file.limit=0 keeps every insert in a new base file).
          spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 1000, 100)")
          spark.sql(s"insert into $tableName values (2, 'a2', 20.0, 1001, 100)")
          spark.sql(s"insert into $tableName values (3, 'a3', 30.0, 1002, 100)")
          // An update lands in a log file, so clustering must merge base + log records through the
          // file group reader using the CUSTOM Spark merger.
          spark.sql(s"update $tableName set price = 99.0, ts = 1003 where id = 1")

          // Pin the row-writer setting and propagate the custom merger impl classes (write-side,
          // non-persisted) to the clustering write client. Before HUDI-18980 the reader properties
          // dropped these and clustering failed with "No valid spark merger implementation set".
          val clusteringOptions =
            s"hoodie.datasource.write.row.writer.enable=$rowWriterEnabled," +
              s"${HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY}=$mergerClass"
          spark.sql(s"call run_clustering(table => '$tableName', options => '$clusteringOptions')").show()

          // Clustering must have produced exactly one completed replace commit.
          val fs = new Path(basePath).getFileSystem(spark.sessionState.newHadoopConf())
          val replaceCommits = HoodieDataSourceHelpers.allCompletedCommitsCompactions(fs, basePath)
            .getInstants.iterator().asScala
            .filter(_.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION)
            .toSeq
          assertResult(1)(replaceCommits.size)

          // All records remain readable after clustering, with the update applied.
          checkAnswer(s"select id, name, price, ts, part from $tableName order by id")(
            Seq(1, "a1", 99.0, 1003, 100),
            Seq(2, "a2", 20.0, 1001, 100),
            Seq(3, "a3", 30.0, 1002, 100)
          )
        }
      }
    }
  }
}

/**
 * A custom Spark record merger whose only purpose is to advertise the CUSTOM merge strategy id, so the
 * table is configured with {@code RecordMergeMode.CUSTOM} and a custom merger impl class. It otherwise
 * reuses the default Spark merge behavior.
 */
class CustomSparkRecordMergerForClustering extends DefaultSparkRecordMerger {
  override def getMergingStrategy: String = HoodieRecordMerger.CUSTOM_MERGE_STRATEGY_UUID
}
