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

package org.apache.hudi.schema;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;

import org.apache.avro.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collections;

import static org.apache.hudi.common.util.ConfigUtils.OLD_SCHEMAPROVIDER_CONFIG_PREFIX;
import static org.apache.hudi.common.util.ConfigUtils.SCHEMAPROVIDER_CONFIG_PREFIX;
import static org.apache.hudi.common.util.ConfigUtils.checkRequiredConfigProperties;
import static org.apache.hudi.common.util.ConfigUtils.containsConfigProperty;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;

/**
 * A simple schema provider, that reads off files on DFS.
 */
public class FilebasedSchemaProvider extends SchemaProvider {

  /**
   * Configs supported.
   */
  public static class Config {
    private static final ConfigProperty<String> SOURCE_SCHEMA_FILE = ConfigProperty
        .key(SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.file")
        .noDefaultValue()
        .withAlternatives(OLD_SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.file")
        .withDocumentation("The schema of the source you are reading from");

    private static final ConfigProperty<String> TARGET_SCHEMA_FILE = ConfigProperty
        .key(SCHEMAPROVIDER_CONFIG_PREFIX + "target.schema.file")
        .noDefaultValue()
        .withAlternatives(OLD_SCHEMAPROVIDER_CONFIG_PREFIX + "target.schema.file")
        .withDocumentation("The schema of the target you are writing to");
  }

  private final Schema sourceSchema;

  private Schema targetSchema;

  @Deprecated
  public FilebasedSchemaProvider(TypedProperties props) {
    checkRequiredConfigProperties(props, Collections.singletonList(Config.SOURCE_SCHEMA_FILE));
    String sourceSchemaFile = getStringWithAltKeys(props, Config.SOURCE_SCHEMA_FILE);
    FileSystem fs = HadoopFSUtils.getFs(sourceSchemaFile, HadoopConfigurations.getHadoopConf(new Configuration()));
    try {
      this.sourceSchema = new Schema.Parser().parse(fs.open(new Path(sourceSchemaFile)));
      if (containsConfigProperty(props, Config.TARGET_SCHEMA_FILE)) {
        this.targetSchema =
            new Schema.Parser().parse(fs.open(new Path(getStringWithAltKeys(props, Config.TARGET_SCHEMA_FILE))));
      }
    } catch (IOException ioe) {
      throw new HoodieIOException("Error reading schema", ioe);
    }
  }

  public FilebasedSchemaProvider(Configuration conf) {
    final String sourceSchemaPath = conf.getString(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH);
    final FileSystem fs = HadoopFSUtils.getFs(sourceSchemaPath, HadoopConfigurations.getHadoopConf(conf));
    try {
      this.sourceSchema = new Schema.Parser().parse(fs.open(new Path(sourceSchemaPath)));
    } catch (IOException ioe) {
      throw new HoodieIOException("Error reading schema", ioe);
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
}
