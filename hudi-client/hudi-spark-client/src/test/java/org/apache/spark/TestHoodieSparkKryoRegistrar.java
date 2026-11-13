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

import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.SerializerInstance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests {@link HoodieSparkKryoRegistrar}
 */
public class TestHoodieSparkKryoRegistrar {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testPayloadKeepsCompactFormAcrossIndependentKryoInstances(boolean deleted) throws IOException {
    Schema writerSchema = SchemaBuilder.record("payload").fields()
        .requiredString("id").requiredLong("ts").requiredString("value").endRecord();
    Schema projection = SchemaBuilder.record("payload").fields().requiredString("value").endRecord();
    GenericRecord record = deleted ? null : new GenericRecordBuilder(writerSchema)
        .set("id", "key").set("ts", 42L).set("value", "updated").build();
    OverwriteWithLatestAvroPayload payload = new OverwriteWithLatestAvroPayload(record, 42L);
    if (!deleted) {
      assertEquals("updated", payload.getInsertValue(projection).get().get(0).toString());
    }
    Kryo writer = newKryo();
    ClassTag<OverwriteWithLatestAvroPayload> tag = ClassTag$.MODULE$.apply(OverwriteWithLatestAvroPayload.class);
    SerializerInstance sparkWithoutRegistrar = new KryoSerializer(new SparkConf(false)).newInstance();
    for (int round = 0; round < 3; round++) {
      try (Output header = new Output(256, -1)) {
        payload.write(writer, header);
        try (Input input = new Input(header.toBytes())) {
          assertEquals(payload.getRecordBytes().length, input.readInt());
        }
      }
      byte[] bytes;
      try (Output output = new Output(256, -1)) {
        writer.writeClassAndObject(output, payload);
        bytes = output.toBytes();
      }
      try (Input input = new Input(bytes)) {
        payload = (OverwriteWithLatestAvroPayload) newKryo().readClassAndObject(input);
      }
      ByteBuffer shuffled = sparkWithoutRegistrar.serialize(payload, tag);
      payload = new KryoSerializer(new SparkConf(false)).newInstance().deserialize(shuffled, tag);
      assertEquals(42L, payload.getOrderingVal());
      if (deleted) {
        assertFalse(payload.getInsertValue(writerSchema).isPresent());
      } else {
        IndexedRecord restored = payload.getInsertValue(writerSchema).get();
        assertEquals("key", restored.get(0).toString());
        assertEquals(42L, restored.get(1));
        assertEquals("updated", restored.get(2).toString());
      }
    }
  }

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
