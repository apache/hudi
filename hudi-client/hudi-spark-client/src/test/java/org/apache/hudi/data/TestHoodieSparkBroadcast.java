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

package org.apache.hudi.data;

import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests {@link HoodieSparkBroadcast}.
 */
class TestHoodieSparkBroadcast {

  /**
   * The value keeps its Java serialization semantics (here a Hadoop conf held in a transient field and
   * written by {@code writeObject}) whichever serializer Spark uses for the broadcast, and the broadcast
   * block holds only bytes, never the original value.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSerializedValueRoundTripsThroughSparkSerializers(boolean useKryo) {
    Configuration hadoopConf = new Configuration(false);
    hadoopConf.set("hoodie.test.broadcast.key", "value1");
    StorageConfiguration<Configuration> storageConf = HadoopFSUtils.getStorageConf(hadoopConf);
    HoodieSparkBroadcast.SerializedValue<StorageConfiguration<Configuration>> serializedValue =
        new HoodieSparkBroadcast.SerializedValue<>(storageConf);

    SparkConf sparkConf = new SparkConf();
    Serializer serializer = useKryo ? new KryoSerializer(sparkConf) : new JavaSerializer(sparkConf);
    SerializerInstance instance = serializer.newInstance();
    ClassTag<HoodieSparkBroadcast.SerializedValue<StorageConfiguration<Configuration>>> tag =
        ClassTag$.MODULE$.apply(HoodieSparkBroadcast.SerializedValue.class);
    assertNull(serializedValue.getCachedValue());
    HoodieSparkBroadcast.SerializedValue<StorageConfiguration<Configuration>> copy =
        instance.deserialize(instance.serialize(serializedValue, tag), tag);

    StorageConfiguration<Configuration> copiedConf = copy.get();
    assertNotSame(storageConf, copiedConf);
    assertEquals("value1", copiedConf.unwrap().get("hoodie.test.broadcast.key"));
    assertEquals("value1", copiedConf.getString("hoodie.test.broadcast.key").get());
  }
}
