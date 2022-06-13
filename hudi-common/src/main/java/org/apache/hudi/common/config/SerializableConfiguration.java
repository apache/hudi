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

package org.apache.hudi.common.config;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * A wrapped configuration which can be serialized.
 */
public class SerializableConfiguration implements Serializable {

  private static final long serialVersionUID = 1L;
  private transient Configuration configuration;

  public SerializableConfiguration(Configuration configuration) {
    this.configuration = new Configuration(configuration);
  }

  public SerializableConfiguration(SerializableConfiguration configuration) {
    this.configuration = configuration.newCopy();
  }

  public Configuration newCopy() {
    return new Configuration(configuration);
  }

  public Configuration get() {
    return configuration;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    configuration.write(out);
  }

  private void readObject(ObjectInputStream in) throws IOException {
    configuration = new Configuration(false);
    configuration.readFields(in);
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    configuration.iterator().forEachRemaining(e -> str.append(String.format("%s => %s \n", e.getKey(), e.getValue())));
    return configuration.toString();
  }

  public static SerializableConfiguration fromProps(Properties props) {
    Configuration hadoopConf = new Configuration();
    props.stringPropertyNames().forEach(k -> hadoopConf.set(k, props.getProperty(k)));
    return new SerializableConfiguration(hadoopConf);
  }
}
