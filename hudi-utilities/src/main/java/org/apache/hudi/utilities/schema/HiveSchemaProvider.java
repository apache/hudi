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
 *
 */

package org.apache.hudi.utilities.schema;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.utilities.config.HiveSchemaProviderConfig;
import org.apache.hudi.utilities.exception.HoodieSchemaFetchException;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;

import static org.apache.hudi.common.util.ConfigUtils.checkRequiredConfigProperties;
import static org.apache.hudi.common.util.ConfigUtils.containsConfigProperty;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;

/**
 * A schema provider to get data schema through user specified hive table.
 */
public class HiveSchemaProvider extends SchemaProvider {

  private final HoodieSchema sourceSchema;
  private HoodieSchema targetSchema;

  public HiveSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    checkRequiredConfigProperties(props, Collections.singletonList(HiveSchemaProviderConfig.SOURCE_SCHEMA_TABLE));
    String sourceSchemaDatabaseName = getStringWithAltKeys(props, HiveSchemaProviderConfig.SOURCE_SCHEMA_DATABASE, true);
    String sourceSchemaTableName = getStringWithAltKeys(props, HiveSchemaProviderConfig.SOURCE_SCHEMA_TABLE);
    SparkSession spark = SparkSession.builder().config(jssc.getConf()).enableHiveSupport().getOrCreate();

    // source schema
    try {
      TableIdentifier sourceSchemaTable = new TableIdentifier(sourceSchemaTableName, scala.Option.apply(sourceSchemaDatabaseName));
      StructType sourceSchema = spark.sessionState().catalog().getTableMetadata(sourceSchemaTable).schema();
      this.sourceSchema = HoodieSchema.fromAvroSchema(AvroConversionUtils.convertStructTypeToAvroSchema(
          sourceSchema,
          sourceSchemaTableName,
          "hoodie." + sourceSchemaDatabaseName));
    } catch (NoSuchTableException | NoSuchDatabaseException e) {
      throw new HoodieSchemaFetchException(String.format("Can't find Hive table: %s.%s", sourceSchemaDatabaseName, sourceSchemaTableName), e);
    }

    // target schema
    if (containsConfigProperty(props, HiveSchemaProviderConfig.TARGET_SCHEMA_TABLE)) {
      String targetSchemaDatabaseName = getStringWithAltKeys(props, HiveSchemaProviderConfig.TARGET_SCHEMA_DATABASE, true);
      String targetSchemaTableName = getStringWithAltKeys(props, HiveSchemaProviderConfig.TARGET_SCHEMA_TABLE);
      try {
        TableIdentifier targetSchemaTable = new TableIdentifier(targetSchemaTableName, scala.Option.apply(targetSchemaDatabaseName));
        StructType targetSchema = spark.sessionState().catalog().getTableMetadata(targetSchemaTable).schema();
        this.targetSchema = HoodieSchema.fromAvroSchema(AvroConversionUtils.convertStructTypeToAvroSchema(
            targetSchema,
            targetSchemaTableName,
            "hoodie." + targetSchemaDatabaseName));
      } catch (NoSuchDatabaseException | NoSuchTableException e) {
        throw new HoodieSchemaFetchException(String.format("Can't find Hive table: %s.%s", targetSchemaDatabaseName, targetSchemaTableName), e);
      }
    }
  }

  @Override
  public HoodieSchema getSourceSchema() {
    return sourceSchema;
  }

  @Override
  public HoodieSchema getTargetSchema() {
    if (targetSchema != null) {
      return targetSchema;
    } else {
      return super.getTargetSchema();
    }
  }
}