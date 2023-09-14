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

package org.apache.hudi.common.model;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.model.debezium.MySqlDebeziumAvroPayload;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;
import org.apache.hudi.metadata.HoodieMetadataPayload;

import static org.apache.hudi.common.table.HoodieTableConfig.PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.PAYLOAD_TYPE;

/**
 * Payload to use for record.
 */
@EnumDescription("Payload to use for merging records")
public enum RecordPayloadType {
  @EnumFieldDescription("Provides support for seamlessly applying changes captured via Amazon Database Migration Service onto S3.")
  AWS_DMS_AVRO(AWSDmsAvroPayload.class.getName()),

  @EnumFieldDescription("A payload to wrap a existing Hoodie Avro Record. Useful to create a HoodieRecord over existing GenericRecords.")
  HOODIE_AVRO(HoodieAvroPayload.class.getName()),

  @EnumFieldDescription("Honors ordering field in both preCombine and combineAndGetUpdateValue.")
  HOODIE_AVRO_DEFAULT(DefaultHoodieRecordPayload.class.getName()),

  @EnumFieldDescription("The only difference with HOODIE_AVRO_DEFAULT is that this does not track the event time metadata for efficiency")
  EVENT_TIME_AVRO(EventTimeAvroPayload.class.getName()),

  @EnumFieldDescription("Subclass of OVERWRITE_LATEST_AVRO used for delta streamer.")
  OVERWRITE_NON_DEF_LATEST_AVRO(OverwriteNonDefaultsWithLatestAvroPayload.class.getName()),

  @EnumFieldDescription("Default payload used for delta streamer.")
  OVERWRITE_LATEST_AVRO(OverwriteWithLatestAvroPayload.class.getName()),

  @EnumFieldDescription("Used for partial update to Hudi Table.")
  PARTIAL_UPDATE_AVRO(PartialUpdateAvroPayload.class.getName()),

  @EnumFieldDescription("Provides support for seamlessly applying changes captured via Debezium for MysqlDB.")
  MYSQL_DEBEZIUM_AVRO(MySqlDebeziumAvroPayload.class.getName()),

  @EnumFieldDescription("Provides support for seamlessly applying changes captured via Debezium for PostgresDB.")
  POSTGRES_DEBEZIUM_AVRO(PostgresDebeziumAvroPayload.class.getName()),

  @EnumFieldDescription("A record payload Hudi's internal metadata table.")
  HOODIE_METADATA(HoodieMetadataPayload.class.getName()),

  @EnumFieldDescription("A record payload to validate the duplicate key for INSERT statement in spark-sql.")
  VALIDATE_DUPLICATE_AVRO("org.apache.spark.sql.hudi.command.ValidateDuplicateKeyPayload"),

  @EnumFieldDescription("A record payload for MERGE INTO statement in spark-sql.")
  EXPRESSION_AVRO("org.apache.spark.sql.hudi.command.payload.ExpressionPayload"),

  @EnumFieldDescription("Use the payload class set in `hoodie.datasource.write.payload.class`")
  CUSTOM("");

  private String className;

  RecordPayloadType(String className) {
    this.className = className;
  }

  public String getClassName() {
    return className;
  }

  public static RecordPayloadType fromClassName(String className) {
    for (RecordPayloadType type : RecordPayloadType.values()) {
      if (type.getClassName().equals(className)) {
        return type;
      }
    }
    // No RecordPayloadType found for class name, return CUSTOM
    CUSTOM.className = className;
    return CUSTOM;
  }

  public static String getPayloadClassName(HoodieConfig config) {
    String payloadClassName;
    if (config.contains(PAYLOAD_CLASS_NAME)) {
      payloadClassName = config.getString(PAYLOAD_CLASS_NAME);
    } else if (config.contains(PAYLOAD_TYPE)) {
      payloadClassName = RecordPayloadType.valueOf(config.getString(PAYLOAD_TYPE)).getClassName();
    } else if (config.contains("hoodie.datasource.write.payload.class")) {
      payloadClassName = config.getString("hoodie.datasource.write.payload.class");
    } else {
      payloadClassName = RecordPayloadType.valueOf(PAYLOAD_TYPE.defaultValue()).getClassName();
    }
    // There could be tables written with payload class from com.uber.hoodie.
    // Need to transparently change to org.apache.hudi.
    return payloadClassName.replace("com.uber.hoodie", "org.apache.hudi");
  }
}
