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

package org.apache.hudi.utilities.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/*
Represents object with all the topic level overrides for multi table delta streamer execution
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableConfig {

  @JsonProperty("database")
  private String database = "";

  @JsonProperty("table_name")
  private String tableName = "";

  @JsonProperty("primary_key_field")
  private String primaryKeyField = "";

  @JsonProperty("partition_key_field")
  private String partitionKeyField;

  @JsonProperty("kafka_topic")
  private String topic = "";

  @JsonProperty("timestamp_type")
  private String partitionTimestampType;

  @JsonProperty("partition_input_format")
  private String partitionInputFormat;

  @JsonProperty("hive_sync_db")
  private String hiveSyncDatabase;

  @JsonProperty("hive_sync_table")
  private String hiveSyncTable;

  @JsonProperty("key_generator_class")
  private String keyGeneratorClassName;

  @JsonProperty("use_pre_apache_input_format")
  private Boolean usePreApacheInputFormatForHiveSync = false;

  @JsonProperty("assume_date_partitioning")
  private Boolean assumeDatePartitioningForHiveSync = false;

  @JsonProperty("target_base_path")
  private String targetBasePath = "";

  public String getTargetBasePath() {
    return targetBasePath;
  }

  public void setTargetBasePath(String targetBasePath) {
    this.targetBasePath = targetBasePath;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getHiveSyncDatabase() {
    return hiveSyncDatabase;
  }

  public void setHiveSyncDatabase(String hiveSyncDatabase) {
    this.hiveSyncDatabase = hiveSyncDatabase;
  }

  public String getHiveSyncTable() {
    return hiveSyncTable;
  }

  public void setHiveSyncTable(String hiveSyncTable) {
    this.hiveSyncTable = hiveSyncTable;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getPrimaryKeyField() {
    return primaryKeyField;
  }

  public void setPrimaryKeyField(String primaryKeyField) {
    this.primaryKeyField = primaryKeyField;
  }

  public String getPartitionKeyField() {
    return partitionKeyField;
  }

  public String getPartitionTimestampType() {
    return partitionTimestampType;
  }

  public void setPartitionTimestampType(String partitionTimestampType) {
    this.partitionTimestampType = partitionTimestampType;
  }

  public String getPartitionInputFormat() {
    return partitionInputFormat;
  }

  public void setPartitionInputFormat(String partitionInputFormat) {
    this.partitionInputFormat = partitionInputFormat;
  }

  public void setPartitionKeyField(String partitionKeyField) {
    this.partitionKeyField = partitionKeyField;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getKeyGeneratorClassName() {
    return keyGeneratorClassName;
  }

  public void setKeyGeneratorClassName(String keyGeneratorClassName) {
    this.keyGeneratorClassName = keyGeneratorClassName;
  }

  public Boolean getUsePreApacheInputFormatForHiveSync() {
    return usePreApacheInputFormatForHiveSync;
  }

  public void setUsePreApacheInputFormatForHiveSync(Boolean usePreApacheInputFormatForHiveSync) {
    this.usePreApacheInputFormatForHiveSync = usePreApacheInputFormatForHiveSync;
  }

  public Boolean getAssumeDatePartitioningForHiveSync() {
    return assumeDatePartitioningForHiveSync;
  }

  public void setAssumeDatePartitioningForHiveSync(Boolean assumeDatePartitioningForHiveSync) {
    this.assumeDatePartitioningForHiveSync = assumeDatePartitioningForHiveSync;
  }

  @Override
  public String toString() {
    return "TableConfig{" + "id='" + database + '\'' + ", primaryKeyField='" + primaryKeyField
      + '\'' + ", partitionKeyField='" + partitionKeyField + '\''
      + ", topic='" + topic + '\''
      + ", partitionTimestampType='" + partitionTimestampType + '\''
      + ", partitionInputFormat='" + partitionInputFormat + '\''
      + ", hiveSyncDatabase='" + hiveSyncDatabase + '\''
      + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TableConfig that = (TableConfig) o;
    return database.equals(that.database) && primaryKeyField.equals(that.primaryKeyField)
      && Objects.equals(partitionKeyField, that.partitionKeyField)
      && Objects.equals(topic, that.topic)
      && Objects.equals(partitionTimestampType, that.partitionTimestampType)
      && Objects.equals(partitionInputFormat, that.partitionInputFormat)
      && Objects.equals(hiveSyncDatabase, that.hiveSyncDatabase)
      && Objects.equals(hiveSyncTable, that.hiveSyncTable);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, primaryKeyField, partitionKeyField, topic, partitionTimestampType,
      partitionInputFormat, hiveSyncDatabase);
  }
}
