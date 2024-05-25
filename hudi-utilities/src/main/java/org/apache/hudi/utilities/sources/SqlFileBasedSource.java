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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Scanner;

import static org.apache.hudi.common.util.ConfigUtils.checkRequiredConfigProperties;
import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.config.SqlFileBasedSourceConfig.EMIT_EPOCH_CHECKPOINT;
import static org.apache.hudi.utilities.config.SqlFileBasedSourceConfig.SOURCE_SQL_FILE;

/**
 * File-based SQL Source that uses SQL queries in a file to read from any table.
 *
 * <p>SQL file path should be configured using this hoodie config:
 *
 * <p>hoodie.streamer.source.sql.file = 'hdfs://xxx/source.sql'
 *
 * <p>File-based SQL Source is used for one time backfill scenarios, this won't update the deltastreamer.checkpoint.key
 * to the processed commit, instead it will fetch the latest successful checkpoint key and set that value as
 * this backfill commits checkpoint so that it won't interrupt the regular incremental processing.
 *
 * <p>To fetch and use the latest incremental checkpoint, you need to also set this hoodie_conf for deltastremer jobs:
 *
 * <p>hoodie.write.meta.key.prefixes = 'deltastreamer.checkpoint.key'
 */
public class SqlFileBasedSource extends RowSource {

  private static final Logger LOG = LoggerFactory.getLogger(SqlFileBasedSource.class);
  private final String sourceSqlFile;
  private final boolean shouldEmitCheckPoint;

  public SqlFileBasedSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    checkRequiredConfigProperties(props, Collections.singletonList(SOURCE_SQL_FILE));
    sourceSqlFile = getStringWithAltKeys(props, SOURCE_SQL_FILE);
    shouldEmitCheckPoint = getBooleanWithAltKeys(props, EMIT_EPOCH_CHECKPOINT);
  }

  @Override
  protected Pair<Option<Dataset<Row>>, String> fetchNextBatch(
      Option<String> lastCkptStr, long sourceLimit) {
    Dataset<Row> rows = null;
    final FileSystem fs = HadoopFSUtils.getFs(sourceSqlFile, sparkContext.hadoopConfiguration(), true);
    try {
      final Scanner scanner = new Scanner(fs.open(new Path(sourceSqlFile)));
      scanner.useDelimiter(";");
      while (scanner.hasNext()) {
        String sqlStr = scanner.next().trim();
        if (!sqlStr.isEmpty()) {
          LOG.info(sqlStr);
          // overwrite the same dataset object until the last statement then return.
          rows = sparkSession.sql(sqlStr);
        }
      }
      return Pair.of(Option.of(rows), shouldEmitCheckPoint ? String.valueOf(System.currentTimeMillis()) : null);
    } catch (IOException ioe) {
      throw new HoodieIOException("Error reading source SQL file.", ioe);
    }
  }
}
