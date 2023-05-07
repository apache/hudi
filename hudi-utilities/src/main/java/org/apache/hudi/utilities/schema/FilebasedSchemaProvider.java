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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.utilities.config.FilebasedSchemaProviderConfig;
import org.apache.hudi.utilities.exception.HoodieSchemaProviderException;
import org.apache.hudi.utilities.sources.helpers.SanitizationUtils;

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

  protected Schema sourceSchema;

  protected Schema targetSchema;

  public FilebasedSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    checkRequiredConfigProperties(props, Collections.singletonList(FilebasedSchemaProviderConfig.SOURCE_SCHEMA_FILE));
    String sourceFile = getStringWithAltKeys(props, FilebasedSchemaProviderConfig.SOURCE_SCHEMA_FILE);
    boolean shouldSanitize = SanitizationUtils.shouldSanitize(props);
    String invalidCharMask = SanitizationUtils.getInvalidCharMask(props);
    this.fs = FSUtils.getFs(sourceFile, jssc.hadoopConfiguration(), true);
    this.sourceSchema = readAvroSchemaFromFile(sourceFile, this.fs, shouldSanitize, invalidCharMask);
    if (containsConfigProperty(props, FilebasedSchemaProviderConfig.TARGET_SCHEMA_FILE)) {
      this.targetSchema = readAvroSchemaFromFile(
          getStringWithAltKeys(props, FilebasedSchemaProviderConfig.TARGET_SCHEMA_FILE),
          this.fs, shouldSanitize, invalidCharMask);
    }
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
    try (FSDataInputStream in = fs.open(new Path(schemaPath))) {
      schemaStr = FileIOUtils.readAsUTFString(in);
    } catch (IOException ioe) {
      throw new HoodieSchemaProviderException(String.format("Error reading schema from file %s", schemaPath), ioe);
    }
    return SanitizationUtils.parseAvroSchema(schemaStr, sanitizeSchema, invalidCharMask);
  }

  // Per write batch, refresh the schema from the file
  @Override
  public void refresh() {
    this.sourceSchema = readAvroSchemaFromFile(sourceFile, this.fs, shouldSanitize, invalidCharMask);
  }
}
