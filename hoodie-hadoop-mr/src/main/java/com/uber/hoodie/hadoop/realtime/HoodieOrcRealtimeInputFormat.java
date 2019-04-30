/*
 * Copyright (c) 2019 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.hadoop.realtime;

import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.hadoop.HoodieOrcInputFormat;
import com.uber.hoodie.hadoop.UseFileSplitsFromInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;

/**
 * ORC Input Format, that provides a real-time view of data in a Hoodie dataset
 */
@UseFileSplitsFromInputFormat
public class HoodieOrcRealtimeInputFormat extends HoodieOrcInputFormat implements Configurable {

  public static final Log LOG = LogFactory.getLog(HoodieOrcRealtimeInputFormat.class);

  // These positions have to be deterministic across all tables
  public static final int HOODIE_COMMIT_TIME_COL_POS = 0;
  public static final int HOODIE_RECORD_KEY_COL_POS = 2;
  public static final int HOODIE_PARTITION_PATH_COL_POS = 3;

  protected Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = addRequiredProjectionFields(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  private static Configuration addRequiredProjectionFields(Configuration configuration) {
    // Need this to do merge records in HoodieRealtimeRecordReader
    configuration = addProjectionField(configuration, HoodieRecord.RECORD_KEY_METADATA_FIELD,
        HOODIE_RECORD_KEY_COL_POS);
    configuration = addProjectionField(configuration, HoodieRecord.COMMIT_TIME_METADATA_FIELD,
        HOODIE_COMMIT_TIME_COL_POS);
    configuration = addProjectionField(configuration, HoodieRecord.PARTITION_PATH_METADATA_FIELD,
        HOODIE_PARTITION_PATH_COL_POS);
    return configuration;
  }

  /**
   * Add a field to the existing fields projected
   */
  private static Configuration addProjectionField(Configuration conf, String fieldName,
      int fieldIndex) {
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
        LOG.debug(String.format(
            "Adding extra column " + fieldName + ", to enable log merging cols (%s) ids (%s) ",
            conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR),
            conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR)));
      }
    }
    return conf;
  }
}