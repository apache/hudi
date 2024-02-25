/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.storage.hadoop;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Implementation of {@link StorageConfiguration} providing Hadoop's {@link Configuration}.
 */
public class HadoopStorageConfiguration extends StorageConfiguration<Configuration> {
  private static final long serialVersionUID = 1L;

  private transient Configuration configuration;

  public HadoopStorageConfiguration() {
    this(new Configuration());
  }

  public HadoopStorageConfiguration(Configuration configuration) {
    this(configuration, false);
  }

  public HadoopStorageConfiguration(Configuration configuration, boolean copy) {
    if (copy) {
      this.configuration = new Configuration(configuration);
    } else {
      this.configuration = configuration;
    }
  }

  public HadoopStorageConfiguration(HadoopStorageConfiguration configuration) {
    this.configuration = configuration.newCopy();
  }

  @Override
  public Configuration get() {
    return configuration;
  }

  @Override
  public Configuration newCopy() {
    return new Configuration(configuration);
  }

  @Override
  public void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    configuration.write(out);
  }

  @Override
  public void readObject(ObjectInputStream in) throws IOException {
    configuration = new Configuration(false);
    configuration.readFields(in);
  }

  @Override
  public void set(String key, String value) {
    configuration.set(key, value);
  }

  @Override
  public Option<String> getString(String key) {
    return Option.ofNullable(configuration.get(key));
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    configuration.iterator().forEachRemaining(
        e -> stringBuilder.append(String.format("%s => %s \n", e.getKey(), e.getValue())));
    return stringBuilder.toString();
  }
}
