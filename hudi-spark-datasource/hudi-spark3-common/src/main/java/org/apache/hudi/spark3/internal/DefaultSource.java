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

package org.apache.hudi.spark3.internal;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.config.HoodieInternalConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.internal.BaseDefaultSource;
import org.apache.hudi.internal.DataSourceInternalWriterHelper;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.DataSourceUtils.tryOverrideParquetWriteLegacyFormatProperty;

/**
 * DataSource V2 implementation for managing internal write logic. Only called internally.
 * This class is only compatible with datasource V2 API in Spark 3.
 */
public class DefaultSource extends BaseDefaultSource implements TableProvider {

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return StructType.fromDDL(options.get(HoodieInternalConfig.BULKINSERT_INPUT_DATA_SCHEMA_DDL.key()));
  }

  @Override
  public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
    String instantTime = properties.get(DataSourceInternalWriterHelper.INSTANT_TIME_OPT_KEY);
    String path = properties.get("path");
    String tblName = properties.get(HoodieWriteConfig.TBL_NAME.key());
    boolean populateMetaFields = Boolean.parseBoolean(properties.getOrDefault(HoodieTableConfig.POPULATE_META_FIELDS.key(),
        Boolean.toString(HoodieTableConfig.POPULATE_META_FIELDS.defaultValue())));
    boolean arePartitionRecordsSorted = Boolean.parseBoolean(properties.getOrDefault(HoodieInternalConfig.BULKINSERT_ARE_PARTITIONER_RECORDS_SORTED,
        Boolean.toString(HoodieInternalConfig.DEFAULT_BULKINSERT_ARE_PARTITIONER_RECORDS_SORTED)));
    // Create a new map as the properties is an unmodifiableMap on Spark 3.2.0
    Map<String, String> newProps = new HashMap<>(properties);
    // Auto set the value of "hoodie.parquet.writelegacyformat.enabled"
    tryOverrideParquetWriteLegacyFormatProperty(newProps, schema);
    // 1st arg to createHoodieConfig is not really required to be set. but passing it anyways.
    HoodieWriteConfig config = DataSourceUtils.createHoodieConfig(newProps.get(HoodieWriteConfig.AVRO_SCHEMA_STRING.key()), path, tblName, newProps);
    return new HoodieDataSourceInternalTable(instantTime, config, schema, getSparkSession(),
        getConfiguration(), newProps, populateMetaFields, arePartitionRecordsSorted);
  }
}
