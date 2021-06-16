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

package org.apache.hudi.utilities.sources.pulsar;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieNotSupportedException;

import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class PulsarUtils {

  static GenericAvroRecord extractValue(Message m, SchemaInfo info) {
    GenericAvroRecord val = null;
    if (info.getType() == SchemaType.AVRO) {
      return (GenericAvroRecord) m.getValue();
    } else if (info.getType() == SchemaType.KEY_VALUE) {
      return ((KeyValue<GenericAvroRecord, GenericAvroRecord>) m.getValue()).getValue();
    } else {
      throw new HoodieNotSupportedException("Unsupported schema type :" + info.getType());
    }
  }

  public static GenericRecord unshadedRecord(org.apache.pulsar.shade.org.apache.avro.generic.GenericRecord shadedGenericRecord) {
    return new UnshadedGenericRecord(shadedGenericRecord);
  }

  static void checkSupportedSchema(SchemaInfo info) {
    ValidationUtils.checkArgument(info.getType() == SchemaType.AVRO || info.getType() == SchemaType.KEY_VALUE,
        "Unsupported schema type :" + info.getType()
    );

    if (info.getType() == SchemaType.KEY_VALUE) {
      KeyValue<SchemaInfo, SchemaInfo> kvInfo = KeyValueSchemaInfo.decodeKeyValueSchemaInfo(info);
      ValidationUtils.checkArgument(kvInfo.getKey().getType() == SchemaType.AVRO
          && kvInfo.getValue().getType() == SchemaType.AVRO, "Only avro key/values supported for key value schemas"
      );
    }
  }

  public static Map<String, MessageId> strToOffsets(String checkpointStr) throws IOException {
    Map<String, MessageId> offsetMap = new HashMap<>();
    String[] splits = checkpointStr.split(",");
    for (String split : splits) {
      String[] subSplits = split.split(":");
      offsetMap.put(subSplits[0], MessageId.fromByteArray(StringUtils.fromHexString(subSplits[1])));
    }
    return offsetMap;
  }

  public static String offsetsToStr(Map<String, MessageId> checkpointOffsets) {
    return checkpointOffsets.entrySet().stream()
        .map(e -> String.format("%s:%s", e.getKey(), StringUtils.toHexString(e.getValue().toByteArray())))
        .collect(Collectors.joining(","));
  }
}
