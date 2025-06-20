/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.config.SqlSourceConfig;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.hudi.common.util.ConfigUtils.checkRequiredConfigProperties;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;

/**
 * SQL Source that reads from any table, used mainly for backfill jobs which will process specific partition dates.
 *
 * <p>Spark SQL should be configured using this hoodie config:
 *
 * <p>hoodie.streamer.source.sql.sql.query = 'select * from source_table'
 *
 * <p>SQL Source is used for one time backfill scenarios, this won't update the deltastreamer.checkpoint.key to the
 * processed commit, instead it will fetch the latest successful checkpoint key and set that value as this backfill
 * commits checkpoint so that it won't interrupt the regular incremental processing.
 *
 * <p>To fetch and use the latest incremental checkpoint, you need to also set this hoodie_conf for deltastremer jobs:
 *
 * <p>hoodie.write.meta.key.prefixes = 'deltastreamer.checkpoint.key'
 *
 * Also, users are expected to set --allow-commit-on-no-checkpoint-change while using this SqlSource.
 */
public class SqlSource extends RowSource {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(SqlSource.class);
  private final String sourceSql;
  private final SparkSession spark;

  public SqlSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    checkRequiredConfigProperties(
        props, Collections.singletonList(SqlSourceConfig.SOURCE_SQL));
    sourceSql = getStringWithAltKeys(props, SqlSourceConfig.SOURCE_SQL);
    spark = sparkSession;
  }

  @Override
  protected Pair<Option<Dataset<Row>>, Checkpoint> fetchNextBatch(
      Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    LOG.debug(sourceSql);
    Dataset<Row> source = spark.sql(sourceSql);
    // Remove Hoodie meta columns except partition path from input source.
    if (Arrays.asList(source.columns()).contains(HoodieRecord.COMMIT_TIME_METADATA_FIELD)) {
      source =
        source.drop(
            HoodieRecord.HOODIE_META_COLUMNS.stream()
                .filter(x -> !x.equals(HoodieRecord.PARTITION_PATH_METADATA_FIELD))
                .toArray(String[]::new));
    }
    return Pair.of(Option.of(source), null);
  }
}
