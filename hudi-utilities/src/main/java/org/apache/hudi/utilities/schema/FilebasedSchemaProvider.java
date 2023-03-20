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

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.sources.helpers.SanitizationUtils;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Collections;

/**
 * A simple schema provider, that reads off files on DFS.
 */
public class FilebasedSchemaProvider extends SchemaProvider {

  /**
   * Configs supported.
   */
  public static class Config {
    private static final String SOURCE_SCHEMA_FILE_PROP = "hoodie.deltastreamer.schemaprovider.source.schema.file";
    private static final String TARGET_SCHEMA_FILE_PROP = "hoodie.deltastreamer.schemaprovider.target.schema.file";
  }

  private final FileSystem fs;

  protected Schema sourceSchema;

  protected Schema targetSchema;

  public FilebasedSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(Config.SOURCE_SCHEMA_FILE_PROP));
    String sourceFile = props.getString(Config.SOURCE_SCHEMA_FILE_PROP);
    boolean shouldSanitize = SanitizationUtils.getShouldSanitize(props);
    String invalidCharMask = SanitizationUtils.getInvalidCharMask(props);
    this.fs = FSUtils.getFs(sourceFile, jssc.hadoopConfiguration(), true);
    this.sourceSchema = readAvroSchemaFromFile(sourceFile, this.fs, shouldSanitize, invalidCharMask);
    if (props.containsKey(Config.TARGET_SCHEMA_FILE_PROP)) {
      this.targetSchema = readAvroSchemaFromFile(props.getString(Config.TARGET_SCHEMA_FILE_PROP), this.fs, shouldSanitize, invalidCharMask);
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
      throw new HoodieIOException(String.format("Error reading schema from file %s", schemaPath), ioe);
    }
    return SanitizationUtils.parseAvroSchema(schemaStr, sanitizeSchema, invalidCharMask);
  }
}
