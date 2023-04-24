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

package org.apache.hudi.common.model;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;

@EnumDescription("Payload to use for record?")
public enum RecordPayloadType {

  @EnumFieldDescription("Provides support for seamlessly applying changes captured via Amazon Database Migration Service onto S3.")
  AWS_DMS_AVRO("org.apache.hudi.common.model.AWSDmsAvroPayload"),

  @EnumFieldDescription("Honors ordering field in both preCombine and combineAndGetUpdateValue.")
  HOODIE_AVRO_DEFAULT("org.apache.hudi.common.model.DefaultHoodieRecordPayload"),

  @EnumFieldDescription("The only difference with HOODIE_AVRO_DEFAULT is that this does not track the event time metadata for efficiency")
  EVENT_TIME_AVRO("org.apache.hudi.common.model.EventTimeAvroPayload"),

  @EnumFieldDescription("Subclass of OVERWRITE_LATEST_AVRO used for delta streamer.")
  OVERWRITE_NON_DEF_LATEST_AVRO("org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload"),

  @EnumFieldDescription("Default payload used for delta streamer.")
  OVERWRITE_LATEST_AVRO("org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"),

  @EnumFieldDescription("Used for partial update to Hudi Table.")
  PARTIAL_UPDATE_AVRO("org.apache.hudi.common.model.PartialUpdateAvroPayload"),

  @EnumFieldDescription("Provides support for seamlessly applying changes captured via Debezium for MysqlDB.")
  MYSQL_DEBEZIUM_AVRO("org.apache.hudi.common.model.debezium.MySqlDebeziumAvroPayload"),

  @EnumFieldDescription("Provides support for seamlessly applying changes captured via Debezium for PostgresDB.")
  POSTGRES_DEBEZIUM_AVRO("org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload"),

  @EnumFieldDescription("Validate the duplicate key for insert statement without enabling the INSERT_DROP_DUPS config.")
  VALIDATE_DUPLICATE_AVRO("org.apache.spark.sql.hudi.command.ValidateDuplicateKeyPayload"),

  @EnumFieldDescription("A HoodieRecordPayload for MergeIntoHoodieTableCommand.")
  EXPRESSION_AVRO("org.apache.spark.sql.hudi.command.payload.ExpressionPayload"),

  @EnumFieldDescription("Use the payload class set in `hoodie.datasource.write.payload.class`")
  CUSTOM("");

  public final String classPath;
  RecordPayloadType(String classPath) {
    this.classPath = classPath;
  }
}
