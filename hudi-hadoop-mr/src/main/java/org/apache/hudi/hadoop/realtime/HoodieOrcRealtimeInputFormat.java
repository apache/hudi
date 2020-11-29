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

package org.apache.hudi.hadoop.realtime;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.hadoop.HoodieOrcInputFormat;
import org.apache.hudi.hadoop.UseFileSplitsFromInputFormat;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

@UseFileSplitsFromInputFormat
public class HoodieOrcRealtimeInputFormat extends HoodieOrcInputFormat implements Configurable {

  private static final Logger LOG = LogManager.getLogger(HoodieOrcRealtimeInputFormat.class);

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
