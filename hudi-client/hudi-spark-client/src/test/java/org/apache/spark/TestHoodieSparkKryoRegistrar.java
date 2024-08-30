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

package org.apache.spark;

import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link HoodieSparkKryoRegistrar}
 */
public class TestHoodieSparkKryoRegistrar {
  @Test
  public void testSerdeHoodieHadoopConfiguration() {
    Kryo kryo = newKryo();

    HadoopStorageConfiguration conf = new HadoopStorageConfiguration(new Configuration());

    // Serialize
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output output = new Output(baos);
    kryo.writeObject(output, conf);
    output.close();

    // Deserialize
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    Input input = new Input(bais);
    HadoopStorageConfiguration deserialized = kryo.readObject(input, HadoopStorageConfiguration.class);
    input.close();

    // Verify
    assertEquals(getPropsInMap(conf), getPropsInMap(deserialized));
  }

  private Kryo newKryo() {
    Kryo kryo = new Kryo();

    // This instance of Kryo should not require prior registration of classes
    kryo.setRegistrationRequired(false);
    kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
    // Handle cases where we may have an odd classloader setup like with libjars
    // for hadoop
    kryo.setClassLoader(Thread.currentThread().getContextClassLoader());

    // Register Hudi's classes
    new HoodieSparkKryoRegistrar().registerClasses(kryo);

    return kryo;
  }

  private Map<String, String> getPropsInMap(HadoopStorageConfiguration conf) {
    Map<String, String> configMap = new HashMap<>();
    conf.unwrap().iterator().forEachRemaining(
        e -> configMap.put(e.getKey(), e.getValue()));
    return configMap;
  }
}
