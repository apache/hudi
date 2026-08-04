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

package org.apache.hudi.hadoop.utils;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeBootstrapBaseFileSplit;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;
import org.apache.hudi.hadoop.realtime.HoodieVirtualKeyInfo;
import org.apache.hudi.hadoop.realtime.RealtimeSplit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.TypeUtils.unsafeCast;

public class HoodieRealtimeInputFormatUtils extends HoodieInputFormatUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieRealtimeInputFormatUtils.class);

  public static boolean doesBelongToIncrementalQuery(FileSplit s) {
    if (s instanceof HoodieRealtimeFileSplit) {
      HoodieRealtimeFileSplit bs = unsafeCast(s);
      return bs.getBelongsToIncrementalQuery();
    } else if (s instanceof HoodieRealtimeBootstrapBaseFileSplit) {
      HoodieRealtimeBootstrapBaseFileSplit bs = unsafeCast(s);
      return bs.getBelongsToIncrementalQuery();
    }

    return false;
  }

  /**
   * Add a field to the existing fields projected.
   */
  private static Configuration addProjectionField(Configuration conf, String fieldName, int fieldIndex) {
    String readColNames = conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "");
    String readColIds = conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "");

    String readColNamesPrefix = readColNames + ",";
    if (readColNames == null || readColNames.isEmpty()) {
      readColNamesPrefix = "";
    }
    String readColIdsPrefix = readColIds + ",";
    if (readColIds == null || readColIds.isEmpty()) {
      readColIdsPrefix = "";
    }

    if (!Arrays.asList(readColNames.split(",")).contains(fieldName)) {
      // If not already in the list - then add it
      conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, readColNamesPrefix + fieldName);
      conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, readColIdsPrefix + fieldIndex);
      LOG.debug("Adding extra column {}, to enable log merging cols ({}) ids ({})",
          fieldName,
          conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR),
          conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));
    }
    return conf;
  }

  public static void addProjectionField(Configuration conf, String[] fieldName) {
    if (fieldName.length > 0) {
      List<String> columnNameList = Arrays.stream(conf.get(serdeConstants.LIST_COLUMNS, "").split(",")).collect(Collectors.toList());
      Arrays.stream(fieldName).forEach(field -> {
        int index = columnNameList.indexOf(field);
        if (index != -1) {
          addProjectionField(conf, field, index);
        }
      });
    }
  }

  public static void addVirtualKeysProjection(Configuration configuration, Option<HoodieVirtualKeyInfo> hoodieVirtualKeyInfo) {
    // Need this to do merge records in HoodieRealtimeRecordReader
    if (!hoodieVirtualKeyInfo.isPresent()) {
      addProjectionField(configuration, HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieInputFormatUtils.HOODIE_RECORD_KEY_COL_POS);
      addProjectionField(configuration, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieInputFormatUtils.HOODIE_COMMIT_TIME_COL_POS);
      addProjectionField(configuration, HoodieRecord.PARTITION_PATH_METADATA_FIELD, HoodieInputFormatUtils.HOODIE_PARTITION_PATH_COL_POS);
    } else {
      HoodieVirtualKeyInfo hoodieVirtualKey = hoodieVirtualKeyInfo.get();
      addProjectionField(configuration, hoodieVirtualKey.getRecordKeyField(), hoodieVirtualKey.getRecordKeyFieldIndex());
      if (hoodieVirtualKey.getPartitionPathField().isPresent()) {
        addProjectionField(configuration, hoodieVirtualKey.getPartitionPathField().get(), hoodieVirtualKey.getPartitionPathFieldIndex().get());
      }
    }
  }

  public static boolean requiredProjectionFieldsExistInConf(Configuration configuration, Option<HoodieVirtualKeyInfo> hoodieVirtualKeyInfo) {
    String readColNames = configuration.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "");
    if (!hoodieVirtualKeyInfo.isPresent()) {
      return readColNames.contains(HoodieRecord.RECORD_KEY_METADATA_FIELD)
          && readColNames.contains(HoodieRecord.COMMIT_TIME_METADATA_FIELD)
          && readColNames.contains(HoodieRecord.PARTITION_PATH_METADATA_FIELD);
    } else {
      return readColNames.contains(hoodieVirtualKeyInfo.get().getRecordKeyField())
          && (!hoodieVirtualKeyInfo.get().getPartitionPathField().isPresent() || readColNames.contains(hoodieVirtualKeyInfo.get().getPartitionPathField().get()));
    }
  }

  public static boolean canAddProjectionToJobConf(final RealtimeSplit realtimeSplit, final JobConf jobConf) {
    return jobConf.get(HoodieInputFormatUtils.HOODIE_READ_COLUMNS_PROP) == null
            || (!realtimeSplit.getDeltaLogPaths().isEmpty() && !HoodieRealtimeInputFormatUtils.requiredProjectionFieldsExistInConf(jobConf, realtimeSplit.getVirtualKeyInfo()));
  }

  /**
   * Drops blank entries from the read-column id list held in {@code conf}.
   *
   * <p>For {@code SELECT COUNT(*)} on Hive before 3.0.0 the read-column ids arrive empty and Hive combines
   * them into e.g. {@code ",2,0,3"} (HIVE-22438). Every consumer parses those ids with
   * {@code Integer#parseInt}, so a blank entry fails with a bare {@code NumberFormatException} that carries
   * none of the projection lists:
   *
   * <ul>
   *   <li>{@code SchemaEvolutionContext#setColumnNameList} and {@code #setColumnTypeList}, reached from both
   *       {@code doEvolutionForParquetFormat} and {@code doEvolutionForRealtimeInputFormat}</li>
   *   <li>{@code HoodieColumnProjectionUtils#getReadColumnIDs}, on the bootstrap path</li>
   *   <li>{@code HoodieRealtimeRecordReaderUtils#orderFields}, the path in the reported issue</li>
   * </ul>
   *
   * <p>Each of those is reached from a {@code getRecordReader} that cleans the conf first:
   * {@code HoodieParquetInputFormat} for the parquet and bootstrap paths, and
   * {@code HoodieParquetRealtimeInputFormat} / {@code HoodieHFileRealtimeInputFormat} for the realtime ones.
   * {@code HoodieCombineHiveInputFormat} builds its per-split readers through those same formats and hands
   * them the conf they cleaned, so it is covered as well.
   *
   * <p>This is a workaround: the underlying bug is in Hive, fixed after 3.0.0, but earlier versions still
   * hit it. Stripping a single leading comma is not enough. Hive prepends ids while appending names, so repeated
   * empty appends give {@code ",,2,0"}, and an id prepended after an empty one gives {@code "3,,2,0"} where
   * the blank is interior and no amount of leading-comma stripping reaches it.
   *
   * <p>Hive on Spark calls {@code getRecordReader} from several threads sharing one {@code JobConf}, so the
   * read-modify-write below is synchronized on the conf the same way {@code addProjectionToJobConf} guards
   * its own writes. The write-back is skipped when nothing changed, so an unset key stays unset rather than
   * being written back as empty.
   */
  public static void cleanProjectionColumnIds(Configuration conf) {
    synchronized (conf) {
      String columnIds = conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "");
      String cleanedColumnIds = Arrays.stream(columnIds.split(","))
          .map(String::trim)
          .filter(id -> !id.isEmpty())
          .collect(Collectors.joining(","));
      if (!cleanedColumnIds.equals(columnIds)) {
        conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, cleanedColumnIds);
        LOG.debug("The projection Ids: {{}} contained blank entries. Cleaned to: {{}}", columnIds, cleanedColumnIds);
      }
    }
  }
}
