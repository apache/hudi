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

package org.apache.hudi.common.util;

import org.apache.avro.util.Utf8;
import org.apache.hudi.avro.model.HoodieReplaceMetadata;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.junit.jupiter.api.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests serialization utils.
 */
public class TestSerializationUtils {

  @Test
  public void testSerDeser() throws IOException {
    // It should handle null object references.
    verifyObject(null);
    // Object with nulls.
    verifyObject(new NonSerializableClass(null));
    // Object with valid values & no default constructor.
    verifyObject(new NonSerializableClass("testValue"));
    // Object with multiple constructor
    verifyObject(new NonSerializableClass("testValue1", "testValue2"));
    // Object which is of non-serializable class.
    verifyObject(new Utf8("test-key"));
    // Verify serialization of list.
    verifyObject(new LinkedList<>(Arrays.asList(2, 3, 5)));
  }

  @Test
  public void testSerDeserReplaceMetadata() throws Exception {
    Random  r = new Random();
    int numPartitions = 1;
    HoodieReplaceMetadata replaceMetadata = new HoodieReplaceMetadata();
    int numFiles = 30;
    Map<String, List<String>> partitionToReplaceFileId = new HashMap<>();

    for (int i = 0; i < numFiles; i++) {
      int partition = r.nextInt(numPartitions);
      String partitionStr = "2020/07/0" + partition;
      partitionToReplaceFileId.putIfAbsent(partitionStr, new ArrayList<>());
      partitionToReplaceFileId.get(partitionStr).add(FSUtils.createNewFileIdPfx());
    }
    String command = "clustering";
    replaceMetadata.setVersion(1);
    replaceMetadata.setTotalFileGroupsReplaced(numFiles);
    replaceMetadata.setCommand(command);
    replaceMetadata.setPartitionMetadata(partitionToReplaceFileId);
    String filePath = "/tmp/myreplacetest";
    byte[] data = TimelineMetadataUtils.serializeReplaceMetadata(replaceMetadata).get();
    FileOutputStream fos = new FileOutputStream(filePath);
    fos.write(data);
    fos.close();

    data = Files.readAllBytes(Paths.get(filePath));
    HoodieReplaceMetadata replaceMetadataD = TimelineMetadataUtils.deserializeHoodieReplaceMetadata(data);
    assertEquals(replaceMetadataD.getTotalFileGroupsReplaced(), numFiles);
    assertEquals(command, replaceMetadataD.getCommand());
    assertEquals(partitionToReplaceFileId, replaceMetadataD.getPartitionMetadata());
  }

  private <T> void verifyObject(T expectedValue) throws IOException {
    byte[] serializedObject = SerializationUtils.serialize(expectedValue);
    assertNotNull(serializedObject);
    assertTrue(serializedObject.length > 0);

    final T deserializedValue = SerializationUtils.<T>deserialize(serializedObject);
    if (expectedValue == null) {
      assertNull(deserializedValue);
    } else {
      assertEquals(expectedValue, deserializedValue);
    }
  }

  private static class NonSerializableClass {
    private String id;
    private String name;

    NonSerializableClass(String id) {
      this(id, "");
    }

    NonSerializableClass(String id, String name) {
      this.id = id;
      this.name = name;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof NonSerializableClass)) {
        return false;
      }
      final NonSerializableClass other = (NonSerializableClass) obj;
      return Objects.equals(this.id, other.id) && Objects.equals(this.name, other.name);
    }
  }
}
