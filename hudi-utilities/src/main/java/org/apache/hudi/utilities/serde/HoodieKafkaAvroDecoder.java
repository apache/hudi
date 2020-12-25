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

package org.apache.hudi.utilities.serde;

import org.apache.hudi.utilities.serde.config.HoodieKafkaAvroDeserializationConfig;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;

public class HoodieKafkaAvroDecoder extends AbstractHoodieKafkaAvroDeserializer implements Decoder<Object> {

  public HoodieKafkaAvroDecoder(VerifiableProperties properties) {
    super(properties);
    configure(new HoodieKafkaAvroDeserializationConfig(properties.props()));
  }

  @Override
  public Object fromBytes(byte[] bytes) {
    return deserialize(bytes);
  }

  @Override
  protected Object deserialize(String topic, Boolean isKey, byte[] payload, Schema readerSchema) throws SerializationException {
    return super.deserialize(topic, isKey, payload, readerSchema);
  }
}
