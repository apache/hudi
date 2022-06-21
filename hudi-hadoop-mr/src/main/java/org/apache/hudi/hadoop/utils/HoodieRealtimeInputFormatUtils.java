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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeBootstrapBaseFileSplit;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;
import org.apache.hudi.hadoop.realtime.HoodieVirtualKeyInfo;
import org.apache.hudi.hadoop.realtime.RealtimeSplit;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import static org.apache.hudi.TypeUtils.unsafeCast;

public class HoodieRealtimeInputFormatUtils extends HoodieInputFormatUtils {

  private static final Logger LOG = LogManager.getLogger(HoodieRealtimeInputFormatUtils.class);

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

    if (!readColNames.contains(fieldName)) {
      // If not already in the list - then add it
      conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, readColNamesPrefix + fieldName);
      conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, readColIdsPrefix + fieldIndex);
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Adding extra column " + fieldName + ", to enable log merging cols (%s) ids (%s) ",
            conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR),
            conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR)));
      }
    }
    return conf;
  }

  public static void addRequiredProjectionFields(Configuration configuration, Option<HoodieVirtualKeyInfo> hoodieVirtualKeyInfo) {
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
          && (hoodieVirtualKeyInfo.get().getPartitionPathField().isPresent() ? readColNames.contains(hoodieVirtualKeyInfo.get().getPartitionPathField().get())
          : true);
    }
  }

  public static boolean canAddProjectionToJobConf(final RealtimeSplit realtimeSplit, final JobConf jobConf) {
    return jobConf.get(HoodieInputFormatUtils.HOODIE_READ_COLUMNS_PROP) == null
            || (!realtimeSplit.getDeltaLogPaths().isEmpty() && !HoodieRealtimeInputFormatUtils.requiredProjectionFieldsExistInConf(jobConf, realtimeSplit.getVirtualKeyInfo()));
  }

  /**
   * Hive will append read columns' ids to old columns' ids during getRecordReader. In some cases, e.g. SELECT COUNT(*),
   * the read columns' id is an empty string and Hive will combine it with Hoodie required projection ids and becomes
   * e.g. ",2,0,3" and will cause an error. Actually this method is a temporary solution because the real bug is from
   * Hive. Hive has fixed this bug after 3.0.0, but the version before that would still face this problem. (HIVE-22438)
   */
  public static void cleanProjectionColumnIds(Configuration conf) {
    String columnIds = conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR);
    if (!columnIds.isEmpty() && columnIds.charAt(0) == ',') {
      conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, columnIds.substring(1));
      if (LOG.isDebugEnabled()) {
        LOG.debug("The projection Ids: {" + columnIds + "} start with ','. First comma is removed");
      }
    }
  }
}
