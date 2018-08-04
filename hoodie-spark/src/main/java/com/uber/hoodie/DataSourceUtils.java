/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie;

import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.util.ReflectionUtils;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieNotSupportedException;
import com.uber.hoodie.index.HoodieIndex;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Utilities used throughout the data source
 */
public class DataSourceUtils {

  /**
   * Obtain value of the provided field as string, denoted by dot notation. e.g: a.b.c
   */
  public static String getNestedFieldValAsString(GenericRecord record, String fieldName) {
    String[] parts = fieldName.split("\\.");
    GenericRecord valueNode = record;
    for (int i = 0; i < parts.length; i++) {
      String part = parts[i];
      Object val = valueNode.get(part);
      if (val == null) {
        break;
      }

      // return, if last part of name
      if (i == parts.length - 1) {
        return val.toString();
      } else {
        // VC: Need a test here
        if (!(val instanceof GenericRecord)) {
          throw new HoodieException("Cannot find a record at part value :" + part);
        }
        valueNode = (GenericRecord) val;
      }
    }
    throw new HoodieException(fieldName + " field not found in record");
  }

  /**
   * Create a key generator class via reflection, passing in any configs needed
   */
  public static KeyGenerator createKeyGenerator(String keyGeneratorClass,
      TypedProperties props) throws IOException {
    try {
      return (KeyGenerator) ReflectionUtils.loadClass(keyGeneratorClass, props);
    } catch (Throwable e) {
      throw new IOException("Could not load key generator class " + keyGeneratorClass, e);
    }
  }

  /**
   * Create a payload class via reflection, passing in an ordering/precombine value.
   */
  public static HoodieRecordPayload createPayload(String payloadClass, GenericRecord record,
      Comparable orderingVal) throws IOException {
    try {
      return (HoodieRecordPayload) ReflectionUtils
          .loadClass(payloadClass, new Class<?>[]{GenericRecord.class, Comparable.class}, record, orderingVal);
    } catch (Throwable e) {
      throw new IOException("Could not create payload for class: " + payloadClass, e);
    }
  }

  public static void checkRequiredProperties(TypedProperties props,
      List<String> checkPropNames) {
    checkPropNames.stream().forEach(prop -> {
      if (!props.containsKey(prop)) {
        throw new HoodieNotSupportedException("Required property " + prop + " is missing");
      }
    });
  }

  public static HoodieWriteClient createHoodieClient(JavaSparkContext jssc, String schemaStr,
      String basePath, String tblName, Map<String, String> parameters) throws Exception {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().combineInput(true, true)
        .withPath(basePath).withAutoCommit(false)
        .withSchema(schemaStr).forTable(tblName).withIndexConfig(
            HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withPayloadClass(parameters.get(
                DataSourceWriteOptions
                    .PAYLOAD_CLASS_OPT_KEY()))
            .build())
        // override above with Hoodie configs specified as options.
        .withProps(parameters).build();

    return new HoodieWriteClient<>(jssc, writeConfig);
  }


  public static JavaRDD<WriteStatus> doWriteOperation(HoodieWriteClient client,
      JavaRDD<HoodieRecord> hoodieRecords, String commitTime, String operation) {
    if (operation.equals(DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL())) {
      return client.bulkInsert(hoodieRecords, commitTime);
    } else if (operation.equals(DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL())) {
      return client.insert(hoodieRecords, commitTime);
    } else {
      //default is upsert
      return client.upsert(hoodieRecords, commitTime);
    }
  }

  public static HoodieRecord createHoodieRecord(GenericRecord gr, Comparable orderingVal,
      HoodieKey hKey, String payloadClass) throws IOException {
    HoodieRecordPayload payload = DataSourceUtils.createPayload(payloadClass, gr, orderingVal);
    return new HoodieRecord<>(hKey, payload);
  }
}
