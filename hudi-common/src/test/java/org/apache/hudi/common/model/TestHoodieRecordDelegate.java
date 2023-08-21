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

package org.apache.hudi.common.model;

import org.apache.hudi.common.testutils.AvroBinaryTestPayload;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.HoodieCommonKryoRegistrar;
import org.apache.hudi.common.util.Option;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieRecordDelegate {
  private HoodieRecordDelegate hoodieRecordDelegate;

  @BeforeEach
  public void setUp() throws Exception {
    SchemaTestUtil testUtil = new SchemaTestUtil();
    final List<IndexedRecord> indexedRecords = testUtil.generateHoodieTestRecords(0, 1);
    final List<HoodieRecord> hoodieRecords =
        indexedRecords.stream().map(r -> new HoodieAvroRecord(new HoodieKey("001", "0000/00/00"),
            new AvroBinaryTestPayload(Option.of((GenericRecord) r)))).collect(Collectors.toList());
    HoodieRecord record = hoodieRecords.get(0);
    record.setCurrentLocation(new HoodieRecordLocation("001", "file01"));
    record.setNewLocation(new HoodieRecordLocation("001", "file-01"));
    hoodieRecordDelegate = HoodieRecordDelegate.fromHoodieRecord(record);
  }

  @Test
  public void testKryoSerializeDeserialize() {
    Kryo kryo = getKryoInstance();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
    kryo.reset();
    baos.reset();
    Output output = new Output(baos);
    hoodieRecordDelegate.write(kryo, output);
    output.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    Input input = new Input(bais);
    hoodieRecordDelegate.read(kryo, input);
    input.close();

    assertEquals(new HoodieKey("001", "0000/00/00"), hoodieRecordDelegate.getHoodieKey());
    assertEquals(new HoodieRecordLocation("001", "file01"), hoodieRecordDelegate.getCurrentLocation().get());
    assertEquals(new HoodieRecordLocation("001", "file-01"), hoodieRecordDelegate.getNewLocation().get());
  }

  public Kryo getKryoInstance() {
    final Kryo kryo = new Kryo();
    // This instance of Kryo should not require prior registration of classes
    kryo.setRegistrationRequired(false);
    kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
    // Handle cases where we may have an odd classloader setup like with libjars
    // for hadoop
    kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
    // Register Hudi's classes
    new HoodieCommonKryoRegistrar().registerClasses(kryo);
    return kryo;
  }
}
