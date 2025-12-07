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

package org.apache.hudi.utilities.schema;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.config.FilebasedSchemaProviderConfig;
import org.apache.hudi.utilities.config.HoodieSchemaProviderConfig;
import org.apache.hudi.utilities.exception.HoodieSchemaProviderException;
import org.apache.hudi.utilities.sources.helpers.SanitizationUtils;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Collections;

import static org.apache.hudi.common.util.ConfigUtils.checkRequiredConfigProperties;
import static org.apache.hudi.common.util.ConfigUtils.containsConfigProperty;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;

/**
 * A simple schema provider, that reads off files on DFS.
 */
public class FilebasedSchemaProvider extends SchemaProvider {

  private final FileSystem fs;

  private final String sourceFile;
  private final String targetFile;

  @Getter
  protected Schema sourceSchema;

  protected Schema targetSchema;

  public FilebasedSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    checkRequiredConfigProperties(props, Collections.singletonList(FilebasedSchemaProviderConfig.SOURCE_SCHEMA_FILE));
    this.sourceFile = getStringWithAltKeys(props, FilebasedSchemaProviderConfig.SOURCE_SCHEMA_FILE);
    this.targetFile = getStringWithAltKeys(props, FilebasedSchemaProviderConfig.TARGET_SCHEMA_FILE, sourceFile);
    this.fs = HadoopFSUtils.getFs(sourceFile, jssc.hadoopConfiguration(), true);
    this.sourceSchema = parseSchema(this.sourceFile);
    if (containsConfigProperty(props, FilebasedSchemaProviderConfig.TARGET_SCHEMA_FILE)) {
      this.targetSchema = parseSchema(this.targetFile);
    }
  }

  private Schema parseSchema(String schemaFile) {
    return readSchemaFromFile(schemaFile, this.fs, config);
  }

  @Override
  public Schema getTargetSchema() {
    if (targetSchema != null) {
      return targetSchema;
    } else {
      return super.getTargetSchema();
    }
  }

  private static Schema readSchemaFromFile(String schemaPath, FileSystem fs, TypedProperties props) {
    return schemaPath.endsWith(".json")
        ? readJsonSchemaFromFile(schemaPath, fs, props)
        : readAvroSchemaFromFile(schemaPath, fs, props);
  }

  private static Schema readJsonSchemaFromFile(String schemaPath, FileSystem fs, TypedProperties props) {
    String schemaConverterClass = getStringWithAltKeys(props, HoodieSchemaProviderConfig.SCHEMA_CONVERTER, true);
    SchemaRegistryProvider.SchemaConverter schemaConverter;
    try {
      ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(schemaConverterClass),
          "Schema converter class must be set for the json file based schema provider");
      schemaConverter = (SchemaRegistryProvider.SchemaConverter) ReflectionUtils.loadClass(
          schemaConverterClass, new Class<?>[] {TypedProperties.class}, props);
    } catch (Exception e) {
      throw new HoodieSchemaProviderException("Error loading json schema converter", e);
    }
    String schemaStr = readSchemaString(schemaPath, fs);
    ParsedSchema parsedSchema = new JsonSchema(schemaStr);
    String convertedSchema;
    try {
      convertedSchema = schemaConverter.convert(parsedSchema);
    } catch (IOException e) {
      throw new HoodieSchemaProviderException(String.format("Error converting json schema from file %s", schemaPath),
          e);
    }
    return new Schema.Parser().parse(convertedSchema);
  }

  private static Schema readAvroSchemaFromFile(String schemaPath,
                                               FileSystem fs,
                                               TypedProperties props) {
    boolean shouldSanitize = SanitizationUtils.shouldSanitize(props);
    String invalidCharMask = SanitizationUtils.getInvalidCharMask(props);
    String schemaStr = readSchemaString(schemaPath, fs);
    return SanitizationUtils.parseAvroSchema(schemaStr, shouldSanitize, invalidCharMask).toAvroSchema();
  }

  private static String readSchemaString(String schemaPath, FileSystem fs) {
    try (FSDataInputStream in = fs.open(new Path(schemaPath))) {
      return FileIOUtils.readAsUTFString(in);
    } catch (IOException ioe) {
      throw new HoodieSchemaProviderException(String.format("Error reading schema from file %s", schemaPath), ioe);
    }
  }

  // Per write batch, refresh the schemas from the file
  @Override
  public void refresh() {
    this.sourceSchema = parseSchema(this.sourceFile);
    this.targetSchema = parseSchema(this.targetFile);
  }
}
