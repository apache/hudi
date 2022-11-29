/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.examples.quickstart.factory;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.examples.quickstart.source.ContinuousFileSource;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.Set;

/**
 * Factory for ContinuousFileSource.
 */
public class ContinuousFileSourceFactory implements DynamicTableSourceFactory {
  public static final String FACTORY_ID = "continuous-file-source";

  public static final ConfigOption<Integer> CHECKPOINTS = ConfigOptions
      .key("checkpoints")
      .intType()
      .defaultValue(2)
      .withDescription("Number of checkpoints to write the data set as, default 2");

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();

    Configuration conf = (Configuration) helper.getOptions();
    Path path = new Path(conf.getOptional(FlinkOptions.PATH).orElseThrow(() ->
        new ValidationException("Option [path] should be not empty.")));
    return new ContinuousFileSource(context.getCatalogTable().getResolvedSchema(), path, conf);
  }

  @Override
  public String factoryIdentifier() {
    return FACTORY_ID;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.singleton(FlinkOptions.PATH);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Collections.singleton(CHECKPOINTS);
  }
}
