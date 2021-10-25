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

package org.apache.hudi.index.simple;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.index.IndexDelegate;
import org.apache.hudi.table.HoodieBaseTable;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.Arrays;
import java.util.stream.Stream;

import scala.collection.Seq;

import static org.apache.hudi.client.utils.ScalaConversions.toSeq;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS;
import static org.apache.hudi.common.model.HoodieRecord.PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.lit;

public class SparkHoodieBroadcastSimpleIndex implements IndexDelegate<Dataset<Row>>, Serializable {

  public static final Column[] JOIN_COLS = Stream.of(PARTITION_PATH_METADATA_FIELD, RECORD_KEY_METADATA_FIELD)
      .map(Column::new).toArray(Column[]::new);
  public static final Seq<String> JOIN_COL_NAMES_SEQ = toSeq(Arrays.stream(JOIN_COLS).map(Column::toString));
  public static final Column[] HOODIE_META_COLS = HOODIE_META_COLUMNS.stream().map(Column::new)
      .toArray(Column[]::new);
  public static final String[] HOODIE_META_COL_NAMES = HOODIE_META_COLUMNS.toArray(new String[0]);

  public static Dataset<Row> addHoodieKeyPartitionCols(Dataset<Row> df) {
    // TODO(rxu) implement keygen options
    return df.withColumn(RECORD_KEY_METADATA_FIELD, callUDF("getKey", lit("uuid"), df.col("uuid")))
        .withColumn(PARTITION_PATH_METADATA_FIELD, callUDF("getPartition", df.col("ts")));
  }

  @Override
  public Dataset<Row> tagLocation(Dataset<Row> upserts, HoodieEngineContext context, HoodieBaseTable table) {
    final Column[] orderedAllCols = Stream
        .concat(Stream.of(HOODIE_META_COL_NAMES), Stream.of(upserts.schema().fieldNames()))
        .map(Column::new).toArray(Column[]::new);

    // broadcast incoming records to join with all existing records
    Dataset<Row> hoodieKeyedUpserts = addHoodieKeyPartitionCols(upserts);

    String[] allBaseFiles = FSUtils
        .getAllPartitionPaths(context, table.getConfig().getMetadataConfig(), table.getMetaClient().getBasePath())
        .stream()
        .flatMap(p -> table.getBaseFileOnlyView().getAllBaseFiles(p).map(BaseFile::getPath))
        .toArray(String[]::new);
    Dataset<Row> allRecords = ((HoodieSparkEngineContext) context).getSqlContext().read()
        .format(table.getBaseFileFormat().name()).load(allBaseFiles);
    Dataset<Row> taggedHoodieMetaColsUpdates = allRecords.select(HOODIE_META_COLS)
        .join(broadcast(hoodieKeyedUpserts.select(JOIN_COLS)), JOIN_COL_NAMES_SEQ);

    // join tagged records back to incoming records
    Dataset<Row> taggedHoodieUpserts = hoodieKeyedUpserts
        .join(taggedHoodieMetaColsUpdates, JOIN_COL_NAMES_SEQ, "left").select(orderedAllCols);

    return taggedHoodieUpserts;
  }

  public boolean rollbackCommit(String commitTime) {
    return true;
  }

  @Override
  public boolean isGlobal() {
    return true;
  }

  public boolean canIndexLogFiles() {
    return false;
  }

  public boolean isImplicitWithStorage() {
    return true;
  }

}
