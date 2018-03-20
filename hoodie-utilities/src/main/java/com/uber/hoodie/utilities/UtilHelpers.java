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

package com.uber.hoodie.utilities;

import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.utilities.exception.HoodieDeltaStreamerException;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import com.uber.hoodie.utilities.sources.Source;
import com.uber.hoodie.utilities.sources.SourceDataFormat;
import java.io.IOException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Bunch of helper methods
 */
public class UtilHelpers {

  public static Source createSource(String sourceClass, PropertiesConfiguration cfg,
      JavaSparkContext jssc, SourceDataFormat dataFormat, SchemaProvider schemaProvider)
      throws IOException {
    try {
      return (Source) ConstructorUtils.invokeConstructor(Class.forName(sourceClass), (Object) cfg,
          (Object) jssc, (Object) dataFormat, (Object) schemaProvider);
    } catch (Throwable e) {
      throw new IOException("Could not load source class " + sourceClass, e);
    }
  }

  public static SchemaProvider createSchemaProvider(String schemaProviderClass,
      PropertiesConfiguration cfg) throws IOException {
    try {
      return (SchemaProvider) ConstructorUtils.invokeConstructor(Class.forName(schemaProviderClass),
          (Object) cfg);
    } catch (Throwable e) {
      throw new IOException("Could not load schema provider class " + schemaProviderClass, e);
    }
  }

  /**
   * TODO: Support hierarchical config files (see CONFIGURATION-609 for sample)
   */
  public static PropertiesConfiguration readConfig(FileSystem fs, Path cfgPath) {
    try {
      FSDataInputStream in = fs.open(cfgPath);
      PropertiesConfiguration config = new PropertiesConfiguration();
      config.load(in);
      in.close();
      return config;
    } catch (IOException e) {
      throw new HoodieIOException("Unable to read config file at :" + cfgPath, e);
    } catch (ConfigurationException e) {
      throw new HoodieDeltaStreamerException("Invalid configs found in config file at :" + cfgPath,
          e);
    }
  }

}
