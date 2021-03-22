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

package org.apache.hudi.utils.factory;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.utils.source.ContinuousFileSource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sources.TableSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory for ContinuousFileSource.
 */
public class ContinuousFileSourceFactory implements TableSourceFactory<RowData> {
  public static final String FACTORY_ID = "continuous-file-source";

  @Override
  public TableSource<RowData> createTableSource(Context context) {
    Configuration conf = FlinkOptions.fromMap(context.getTable().getOptions());
    Path path = new Path(conf.getOptional(FlinkOptions.PATH).orElseThrow(() ->
        new ValidationException("Option [path] should be not empty.")));
    return new ContinuousFileSource(context.getTable().getSchema(), path, conf);
  }

  @Override
  public Map<String, String> requiredContext() {
    Map<String, String> context = new HashMap<>();
    context.put(FactoryUtil.CONNECTOR.key(), FACTORY_ID);
    return context;
  }

  @Override
  public List<String> supportedProperties() {
    return Collections.singletonList("*");
  }
}
