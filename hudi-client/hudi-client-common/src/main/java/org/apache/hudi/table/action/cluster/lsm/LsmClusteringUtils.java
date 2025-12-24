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

package org.apache.hudi.table.action.cluster.lsm;

import org.apache.hudi.common.model.lsm.HoodieLSMLogFile;
import org.apache.hudi.common.util.BaseFileUtils;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.util.EnumSet;

public class LsmClusteringUtils {

  private static final EnumSet<Schema.Type> NUMERIC_TYPES = EnumSet.of(
      Schema.Type.INT,
      Schema.Type.LONG,
      Schema.Type.FLOAT,
      Schema.Type.DOUBLE
  );

  public static DataFile buildDataFile(BaseFileUtils parquetUtils, HoodieLSMLogFile logFile, Configuration conf) {
    if (logFile.getMax() != null) {
      return new DataFile(logFile.getLevelNumber(), logFile.getMin(), logFile.getMax(), logFile.getFileSize(), logFile.getPath().toString(), logFile);
    } else {
      FileStatus logStatus = logFile.getFileStatus();
      Path logPath = logStatus.getPath();
      String[] minMax = parquetUtils.readMinMaxRecordKeys(conf, logPath);
      int levelNumber = logFile.getLevelNumber();
      return new DataFile(levelNumber, minMax[0], minMax[1], logStatus.getLen(), logPath.toUri().toString(), logFile);
    }
  }

  public static boolean isNumericType(Schema.Field field) {
    if (field == null) {
      return false;
    }

    Schema fieldSchema = field.schema();
    Schema.Type fieldType = fieldSchema.getType();

    // 处理单类型和union类型
    if (fieldType == Schema.Type.UNION) {
      // 检查union中的所有可能类型
      for (Schema unionType : fieldSchema.getTypes()) {
        if (isPrimitiveNumericType(unionType.getType())) {
          return true;
        }
      }
      return false;
    } else {
      // 处理非union类型
      return isPrimitiveNumericType(fieldType);
    }
  }

  private static boolean isPrimitiveNumericType(Schema.Type type) {
    return NUMERIC_TYPES.contains(type);
  }
}
