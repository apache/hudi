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

import org.apache.hudi.hadoop.fs.inline.InLineFileSystem;
import org.apache.hudi.io.storage.BaseTestStorageConfiguration;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link HadoopStorageConfiguration}.
 */
public class TestHadoopStorageConfiguration extends BaseTestStorageConfiguration<Configuration> {
  // A HadoopStorageConfiguration followed by the string "trailer", serialized by the previous
  // implementation that delegated to Configuration#write.
  private static final String LEGACY_SERIALIZED_BASE64 = "rO0ABXNyADlvcmcuYXBhY2hlLmh1ZGkuc3RvcmFnZS5oYWRvb3AuSGFkb29wU3RvcmFnZUNvbmZpZ3VyYXRpb24A"
      + "AAAAAAAAAQMAAHhyACxvcmcuYXBhY2hlLmh1ZGkuc3RvcmFnZS5TdG9yYWdlQ29uZmlndXJhdGlvbrLcOWU5Fg+x"
      + "AgAAeHB6AAABEwQPaG9vZGllLnRlc3Qua2V5BnZhbHVlMQAAAAEAAAAjH4sIAAAAAAAA/ysoyk8vSsxNLMlMTszJ"
      + "qQQAw45JTQ8AAAAMZnMuZGVmYXVsdEZTEmZpbGU6Ly8vZGVwcmVjYXRlZAAAAAEAAAA4H4sIAAAAAAAA/0tKTU4s"
      + "LU5VSCvWS0lNSyzNKdHLS8xNVcgsVkhJLSgCypakpgAA6rw5XCUAAAAPZnMuZGVmYXVsdC5uYW1lEmZpbGU6Ly8v"
      + "ZGVwcmVjYXRlZP////8TaG9vZGllLnRlc3QudW5pY29kZQnDqeS4rfCfmIAAAAABAAAAIx+LCAAAAAAAAP8rKMpP"
      + "L0rMTSzJTE7MyakEAMOOSU0PAAAAeHQAB3RyYWlsZXI=";

  @Override
  protected StorageConfiguration<Configuration> getStorageConfiguration(Configuration conf) {
    return new HadoopStorageConfiguration(conf);
  }

  @Override
  protected Configuration getConf(Map<String, String> mapping) {
    Configuration conf = new Configuration();
    mapping.forEach(conf::set);
    return conf;
  }

  /**
   * Verifies getInline() derives from this object's underlying config (caller-supplied
   * configs like S3 credentials carry over) as an independent copy, with the inline FS
   * implementation registered.
   */
  @Test
  public void testGetInlineCarriesOverExistingConfigs() {
    Configuration conf = new Configuration();
    conf.set("fs.s3a.access.key", "test-access-key");
    StorageConfiguration<Configuration> storageConf = new HadoopStorageConfiguration(conf);

    StorageConfiguration<Configuration> inlineConf = storageConf.getInline();
    assertEquals("test-access-key", inlineConf.getString("fs.s3a.access.key").get());
    assertEquals(InLineFileSystem.class.getName(),
        inlineConf.getString("fs." + InLineFileSystem.SCHEME + ".impl").get());

    // the inline conf is a copy; mutating it must not leak back into the source conf
    inlineConf.set("fs.s3a.access.key", "mutated");
    assertEquals("test-access-key", storageConf.getString("fs.s3a.access.key").get());
  }

  /**
   * Verifies Java serialization keeps every property, including defaults, values longer
   * than 64KB and keys set through a deprecated name.
   */
  @Test
  public void testSerializationRoundTripKeepsAllProperties() throws Exception {
    Configuration conf = new Configuration();
    StringBuilder longValue = new StringBuilder();
    for (int i = 0; i < 70_000; i++) {
      longValue.append((char) ('a' + i % 26));
    }
    conf.set("hoodie.test.long.value", longValue.toString());
    conf.set("hoodie.test.unicode.value", new String(new int[] {0xe9, 0x4e2d, 0x1f600}, 0, 3));
    conf.set("fs.default.name", "file:///deprecated");

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bytes)) {
      oos.writeObject(new HadoopStorageConfiguration(conf));
    }
    Configuration deserialized;
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      deserialized = ((HadoopStorageConfiguration) ois.readObject()).unwrap();
    }

    Map<String, String> expected = new TreeMap<>();
    conf.forEach(e -> expected.put(e.getKey(), e.getValue()));
    Map<String, String> actual = new TreeMap<>();
    deserialized.forEach(e -> actual.put(e.getKey(), e.getValue()));
    assertEquals(expected, actual);
    assertEquals("file:///deprecated", deserialized.get("fs.defaultFS"));
    assertEquals(longValue.toString(), deserialized.get("hoodie.test.long.value"));
  }

  /**
   * Verifies payloads written by the previous Configuration#write based implementation (e.g. in
   * Flink state from an older Hudi version) are still read, leaving the stream correctly positioned.
   */
  @Test
  public void testDeserializesLegacyFormat() throws Exception {
    byte[] legacy = Base64.getDecoder().decode(LEGACY_SERIALIZED_BASE64);
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(legacy))) {
      Configuration conf = ((HadoopStorageConfiguration) ois.readObject()).unwrap();
      assertEquals("value1", conf.get("hoodie.test.key"));
      assertEquals("file:///deprecated", conf.get("fs.defaultFS"));
      assertEquals("file:///deprecated", conf.get("fs.default.name"));
      assertEquals(new String(new int[] {0xe9, 0x4e2d, 0x1f600}, 0, 3), conf.get("hoodie.test.unicode"));
      assertEquals(4, conf.size());
      assertEquals("trailer", ois.readObject());
    }
  }
}
