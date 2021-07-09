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

package org.apache.hudi.common.util.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.StringSerializer;

import org.apache.hudi.common.model.HoodieKey;

/**
 * Serializer for HoodieKey V1.
 */
public class HoodieKeyV1Serializer extends Serializer {

  @Override
  public void write(Kryo kryo, Output output, Object object) {
    throw new UnsupportedOperationException("Latest serializer should be used to serialize HoodieKey.");
  }

  @Override
  public Object read(Kryo kryo, Input input, Class type) {
    String partitionPath = kryo.readObjectOrNull(input, String.class, new StringSerializer());
    String recordKey = kryo.readObjectOrNull(input, String.class, new StringSerializer());
    return new HoodieKey(recordKey, partitionPath);
  }
}
