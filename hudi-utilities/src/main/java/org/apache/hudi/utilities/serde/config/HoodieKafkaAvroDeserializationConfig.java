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

package org.apache.hudi.utilities.serde.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class HoodieKafkaAvroDeserializationConfig extends AbstractConfig {

  public static final String SPECIFIC_AVRO_READER_CONFIG = "specific.avro.reader";
  public static final boolean SPECIFIC_AVRO_READER_DEFAULT = false;
  public static final String SPECIFIC_AVRO_READER_DOC = "If true, tries to look up the SpecificRecord class ";

  private static ConfigDef config;

  static {
    config = new ConfigDef()
      .define(SPECIFIC_AVRO_READER_CONFIG, ConfigDef.Type.BOOLEAN, SPECIFIC_AVRO_READER_DEFAULT,
        ConfigDef.Importance.LOW, SPECIFIC_AVRO_READER_DOC);
  }

  public HoodieKafkaAvroDeserializationConfig(Map<?,?> props) {
    super(config, props);
  }

}
