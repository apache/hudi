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

package org.apache.hudi.util;

import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utilities for HoodieTableFactory sanity check.
 */
public class SanityChecks {

  private static final Logger LOG = LoggerFactory.getLogger(SanityChecks.class);

  /**
   * The sanity check.
   *
   * @param conf  The table options
   * @param schema  The table schema
   * @param checkMetaData  Weather to check metadata
   */
  public static void sanitCheck(Configuration conf, ResolvedSchema schema, Boolean checkMetaData) {
    checkTableType(conf);
    List<String> schemaFields = schema.getColumnNames();
    if (checkMetaData) {
      HoodieTableMetaClient metaClient = StreamerUtil.metaClientForReader(conf, HadoopConfigurations.getHadoopConf(conf));
      List<String> latestTablefields = StreamerUtil.getLatestTableFields(metaClient);
      if (latestTablefields != null) {
        checkSchema(conf, schemaFields, latestTablefields);
        checkRecordKey(conf, latestTablefields);
      }
    } else {
      if (!schema.getPrimaryKey().isPresent()) {
        checkRecordKey(conf, schemaFields);
      }
      checkIndexType(conf);
      checkPreCombineKey(conf, schema.getColumnNames());
    }
    Log.info("The sanity check for table " + conf.getString(FlinkOptions.TABLE_NAME) + " was successful.");
  }

  /**
   * Validate the table type.
   */
  public static void checkTableType(Configuration conf) {
    try {
      HoodieTableType.valueOf(conf.get(FlinkOptions.TABLE_TYPE));
    } catch (IllegalArgumentException e) {
      throw new HoodieValidationException("Invalid table type in the table " + conf.getString(FlinkOptions.TABLE_NAME)
         + ". Table type should be either " + HoodieTableType.MERGE_ON_READ + " or " + HoodieTableType.COPY_ON_WRITE + ".");
    }
  }

  /**
   * Validate the table schema.
   */
  public static void checkSchema(Configuration conf, List<String> fields, List<String> lastestTableFields) {
    if (lastestTableFields != null) {
      List<String> missingFields = fields.stream().filter(field -> !lastestTableFields.contains(field)).collect(Collectors.toList());
      if (!missingFields.isEmpty()) {
        throw new HoodieValidationException("Column(s) [" + String.join(", ", missingFields) + "] does not exist in the table " + conf.getString(FlinkOptions.TABLE_NAME) + ".");
      }
    }
  }

  /**
   * Validate the index type.
   */
  public static void checkIndexType(Configuration conf) {
    if (OptionsResolver.isAppendMode(conf)) {
      return;
    }
    String indexType = conf.get(FlinkOptions.INDEX_TYPE);
    if (!StringUtils.isNullOrEmpty(indexType)) {
      try {
        HoodieIndexConfig.INDEX_TYPE.checkValues(indexType);
      } catch (IllegalArgumentException e) {
        throw new HoodieValidationException("Invalid table index in the table " + conf.getString(FlinkOptions.TABLE_NAME) + " : " + e.getMessage());
      }
    }
  }

  /**
   * Validate the record key(s).
   */
  public static void checkRecordKey(Configuration conf,List<String> existingFields) {
    if (OptionsResolver.isAppendMode(conf)) {
      return;
    }
    String[] recordKeys = conf.getString(FlinkOptions.RECORD_KEY_FIELD).split(",");
    final List<String> expColumns = Arrays.stream(recordKeys)
        .filter(key -> !existingFields.contains(key))
        .collect(Collectors.toList());

    if (!expColumns.isEmpty()) {
      if (expColumns.size() == 1 && FlinkOptions.RECORD_KEY_FIELD.defaultValue().equals(expColumns.get(0))) {
        throw new HoodieValidationException("Record key definition is required in the table " + conf.getString(FlinkOptions.TABLE_NAME)
           + ", the default primary key field '" + FlinkOptions.RECORD_KEY_FIELD.defaultValue() + "' does not exist, "
           + "use either PRIMARY KEY syntax or option '" + FlinkOptions.RECORD_KEY_FIELD.key() + "' to speciy.");
      } else {
        throw new HoodieValidationException("Record Key(s) [" + String.join(", ", expColumns) + "] specified in option "
           + FlinkOptions.RECORD_KEY_FIELD.key() + " does not exist in the table "
           + conf.getString(FlinkOptions.TABLE_NAME) + ".");
      }
    }
  }

  /**
   * Validate pre_combine key.
   */
  public static void checkPreCombineKey(Configuration conf, List<String> fields) {
    String preCombineField = conf.get(FlinkOptions.PRECOMBINE_FIELD);
    if (!fields.contains(preCombineField)) {
      if (OptionsResolver.isDefaultHoodieRecordPayloadClazz(conf)) {
        throw new HoodieValidationException("Option '" + FlinkOptions.PRECOMBINE_FIELD.key()
           + "' is required for payload class: " + DefaultHoodieRecordPayload.class.getName()
           + " in the table " + conf.getString(FlinkOptions.TABLE_NAME) + ".");
      }
      if (preCombineField.equals(FlinkOptions.PRECOMBINE_FIELD.defaultValue())) {
        conf.setString(FlinkOptions.PRECOMBINE_FIELD, FlinkOptions.NO_PRE_COMBINE);
      } else if (!preCombineField.equals(FlinkOptions.NO_PRE_COMBINE)) {
        throw new HoodieValidationException("Field " + preCombineField + " does not exist in the table "
           + conf.getString(FlinkOptions.TABLE_NAME) + ".Please check '" + FlinkOptions.PRECOMBINE_FIELD.key() + "' option.");
      }
    }
  }

  /**
   * Validate keygen generator.
   */
  public static void checkKeygenGenerator(boolean isComplexHoodieKey, Configuration conf) {
    if (isComplexHoodieKey && FlinkOptions.isDefaultValueDefined(conf, FlinkOptions.KEYGEN_CLASS_NAME)) {
      conf.setString(FlinkOptions.KEYGEN_CLASS_NAME, ComplexAvroKeyGenerator.class.getName());
      LOG.info("Table option [{}] is reset to {}  in the table {}, because record key or partition path has two or more fields",
          FlinkOptions.KEYGEN_CLASS_NAME.key(), ComplexAvroKeyGenerator.class.getName(), conf.getString(FlinkOptions.TABLE_NAME));
    }
  }
}