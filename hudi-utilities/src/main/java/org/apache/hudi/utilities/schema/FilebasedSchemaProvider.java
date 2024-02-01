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
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.config.FilebasedSchemaProviderConfig;
import org.apache.hudi.utilities.exception.HoodieSchemaProviderException;
import org.apache.hudi.utilities.sources.helpers.SanitizationUtils;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.InputStream;
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
  private final boolean shouldSanitize;
  private final String invalidCharMask;

  protected Schema sourceSchema;

  protected Schema targetSchema;

  public FilebasedSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    checkRequiredConfigProperties(props, Collections.singletonList(FilebasedSchemaProviderConfig.SOURCE_SCHEMA_FILE));
    this.sourceFile = getStringWithAltKeys(props, FilebasedSchemaProviderConfig.SOURCE_SCHEMA_FILE);
    this.targetFile = getStringWithAltKeys(props, FilebasedSchemaProviderConfig.TARGET_SCHEMA_FILE, sourceFile);
    this.shouldSanitize = SanitizationUtils.shouldSanitize(props);
    this.invalidCharMask = SanitizationUtils.getInvalidCharMask(props);
    this.fs = HadoopFSUtils.getFs(sourceFile, jssc.hadoopConfiguration(), true);
    this.sourceSchema = parseSchema(this.sourceFile);
    if (containsConfigProperty(props, FilebasedSchemaProviderConfig.TARGET_SCHEMA_FILE)) {
      this.targetSchema = parseSchema(this.targetFile);
    }
  }

  private Schema parseSchema(String schemaFile) {
    return readAvroSchemaFromFile(schemaFile, this.fs, shouldSanitize, invalidCharMask);
  }

  @Override
  public Schema getSourceSchema() {
    return sourceSchema;
  }

  @Override
  public Schema getTargetSchema() {
    if (targetSchema != null) {
      return targetSchema;
    } else {
      return super.getTargetSchema();
    }
  }

  private static Schema readAvroSchemaFromFile(String schemaPath, FileSystem fs, boolean sanitizeSchema, String invalidCharMask) {
    String schemaStr;
    try (InputStream in = fs.open(new Path(schemaPath))) {
      schemaStr = FileIOUtils.readAsUTFString(in);
    } catch (IOException ioe) {
      throw new HoodieSchemaProviderException(String.format("Error reading schema from file %s", schemaPath), ioe);
    }
    return SanitizationUtils.parseAvroSchema(schemaStr, sanitizeSchema, invalidCharMask);
  }

  // Per write batch, refresh the schemas from the file
  @Override
  public void refresh() {
    this.sourceSchema = parseSchema(this.sourceFile);
    this.targetSchema = parseSchema(this.targetFile);
  }
}
