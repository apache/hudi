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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests the sort-mode based selection in {@link JavaBulkInsertInternalPartitionerFactory}.
 */
public class TestJavaBulkInsertInternalPartitionerFactory {

  @Test
  void noneModeReturnsNonSortPartitioner() {
    BulkInsertPartitioner partitioner = JavaBulkInsertInternalPartitionerFactory.get(BulkInsertSortMode.NONE);
    assertInstanceOf(JavaNonSortPartitioner.class, partitioner);
  }

  @Test
  void globalSortModeReturnsGlobalSortPartitioner() {
    BulkInsertPartitioner partitioner = JavaBulkInsertInternalPartitionerFactory.get(BulkInsertSortMode.GLOBAL_SORT);
    assertInstanceOf(JavaGlobalSortPartitioner.class, partitioner);
  }

  /**
   * A partitioner named through
   * {@code HoodieWriteConfig.BULKINSERT_USER_DEFINED_PARTITIONER_CLASS_NAME} is instantiated by
   * reflection with only the write config, so every partitioner backing an out of the box sort
   * mode has to expose a constructor taking only one. See HUDI-7526.
   */
  @ParameterizedTest
  @ValueSource(classes = {JavaNonSortPartitioner.class, JavaGlobalSortPartitioner.class})
  void sortModePartitionersTakeWriteConfig(Class<?> partitionerClass) {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/").build();
    Object partitioner = ReflectionUtils.loadClass(partitionerClass.getName(), config);
    assertInstanceOf(BulkInsertPartitioner.class, partitioner);
  }

  /**
   * Declaring the write config constructor drops the implicit no-arg one, which the factory uses.
   */
  @ParameterizedTest
  @ValueSource(classes = {JavaNonSortPartitioner.class, JavaGlobalSortPartitioner.class})
  void sortModePartitionersKeepNoArgConstructor(Class<?> partitionerClass) {
    assertInstanceOf(BulkInsertPartitioner.class, ReflectionUtils.loadClass(partitionerClass.getName()));
  }

  @Test
  void unsupportedModeThrows() {
    assertThrows(HoodieException.class,
        () -> JavaBulkInsertInternalPartitionerFactory.get(BulkInsertSortMode.PARTITION_SORT));
    assertThrows(HoodieException.class,
        () -> JavaBulkInsertInternalPartitionerFactory.get(BulkInsertSortMode.PARTITION_PATH_REPARTITION));
  }
}
