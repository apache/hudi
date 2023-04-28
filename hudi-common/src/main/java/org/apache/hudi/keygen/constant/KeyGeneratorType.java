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

package org.apache.hudi.keygen.constant;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Types of {@link org.apache.hudi.keygen.KeyGenerator}.
 */
@EnumDescription("Key generator type, indicating the key generator class to use, that implements "
    + "`org.apache.hudi.keygen.KeyGenerator`.")
public enum KeyGeneratorType {

  @EnumFieldDescription("Simple key generator, which takes names of fields to be used for recordKey and partitionPath as configs.")
  SIMPLE,

  @EnumFieldDescription("Complex key generator, which takes names of fields to be used for recordKey and partitionPath as configs.")
  COMPLEX,

  @EnumFieldDescription("Timestamp-based key generator, that relies on timestamps for partitioning field. Still picks record key by name.")
  TIMESTAMP,

  @EnumFieldDescription("This is a generic implementation type of KeyGenerator where users can configure record key as a single field or "
      + " a combination of fields. Similarly partition path can be configured to have multiple fields or only one field. "
      + " This KeyGenerator expects value for prop \"hoodie.datasource.write.partitionpath.field\" in a specific format. "
      + " For example: "
      + " properties.put(\"hoodie.datasource.write.partitionpath.field\", \"field1:PartitionKeyType1,field2:PartitionKeyType2\").")
  CUSTOM,

  @EnumFieldDescription("Simple Key generator for non-partitioned tables.")
  NON_PARTITION,

  @EnumFieldDescription("Key generator for deletes using global indices.")
  GLOBAL_DELETE;

  public static List<String> getNames() {
    List<String> names = new ArrayList<>(KeyGeneratorType.values().length);
    Arrays.stream(KeyGeneratorType.values())
        .forEach(x -> names.add(x.name()));
    return names;
  }
}
