/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.utilities.schema;

import com.uber.hoodie.DataSourceUtils;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.exception.HoodieIOException;
import java.io.IOException;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A simple schema provider, that reads off files on DFS
 */
public class FilebasedSchemaProvider extends SchemaProvider {

  /**
   * Configs supported
   */
  static class Config {

    private static final String SOURCE_SCHEMA_FILE_PROP = "hoodie.deltastreamer.filebased"
                                                              + ".schemaprovider.source.schema"
                                                              + ".file";
    private static final String TARGET_SCHEMA_FILE_PROP = "hoodie.deltastreamer.filebased"
                                                              + ".schemaprovider.target.schema"
                                                              + ".file";
  }

  private final FileSystem fs;

  private final Schema sourceSchema;

  private final Schema targetSchema;

  public FilebasedSchemaProvider(PropertiesConfiguration config) {
    super(config);
    this.fs = FSUtils.getFs(config.getBasePath(), new Configuration());

    DataSourceUtils.checkRequiredProperties(config,
        Arrays.asList(Config.SOURCE_SCHEMA_FILE_PROP, Config.TARGET_SCHEMA_FILE_PROP));
    try {
      this.sourceSchema = new Schema.Parser().parse(
          fs.open(new Path(config.getString(Config.SOURCE_SCHEMA_FILE_PROP))));
      this.targetSchema = new Schema.Parser().parse(
          fs.open(new Path(config.getString(Config.TARGET_SCHEMA_FILE_PROP))));
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
    return targetSchema;
  }
}
