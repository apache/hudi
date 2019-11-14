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

package org.apache.hudi.client.utils;

import org.apache.hudi.common.model.TimelineLayoutVersion;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.stream.Collectors;

public class ClientUtils {

  /**
   * Create Consistency Aware MetaClient.
   *
   * @param jsc JavaSparkContext
   * @param config HoodieWriteConfig
   * @param loadActiveTimelineOnLoad early loading of timeline
   */
  public static HoodieTableMetaClient createMetaClient(JavaSparkContext jsc, HoodieWriteConfig config,
      boolean loadActiveTimelineOnLoad) {
    return new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), loadActiveTimelineOnLoad,
        config.getConsistencyGuardConfig(), Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion())));
  }

  /**
   * Obtain value of the provided field as string, denoted by dot notation. e.g: a.b.c
   */
  public static String getNestedFieldValAsString(GenericRecord record, String fieldName) {
    Object obj = getNestedFieldVal(record, fieldName);
    return (obj == null) ? null : obj.toString();
  }

  /**
   * Obtain value of the provided field, denoted by dot notation. e.g: a.b.c
   */
  public static Object getNestedFieldVal(GenericRecord record, String fieldName) {
    String[] parts = fieldName.split("\\.");
    GenericRecord valueNode = record;
    int i = 0;
    for (; i < parts.length; i++) {
      String part = parts[i];
      Object val = valueNode.get(part);
      if (val == null) {
        break;
      }

      // return, if last part of name
      if (i == parts.length - 1) {
        return val;
      } else {
        // VC: Need a test here
        if (!(val instanceof GenericRecord)) {
          throw new HoodieException("Cannot find a record at part value :" + part);
        }
        valueNode = (GenericRecord) val;
      }
    }
    throw new HoodieException(
        fieldName + "(Part -" + parts[i] + ") field not found in record. Acceptable fields were :"
            + valueNode.getSchema().getFields().stream().map(Field::name).collect(Collectors.toList()));
  }
}
