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

package org.apache.hudi.utilities.unstructured.embedding.source;

import org.apache.spark.sql.hudi.blob.BatchedBlobReader;
import org.junit.jupiter.api.Test;

import scala.Option;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestBlobReferencePathVersion {
  @Test
  void unmanagedSchemeLessPathsResolveAgainstTheTable() {
    assertEquals("s3a://bucket/table/rel/a.bin",
        BatchedBlobReader.resolveReferencePath("rel/a.bin", false, Option.apply("s3a://bucket/table"), Option.apply("1")));
  }

  @Test
  void managedPathControlAndSchemedPaths() {
    assertEquals("s3a://bucket/table/rel/a.bin",
        BatchedBlobReader.resolveReferencePath("rel/a.bin", true, Option.apply("s3a://bucket/table"), Option.apply("1")));
    assertEquals("s3a://bucket/table/rel/a.bin",
        BatchedBlobReader.resolveReferencePath("/rel/a.bin", false, Option.apply("s3a://bucket/table"), Option.apply("1")));
    assertEquals("s3a://other/key", BatchedBlobReader.resolveReferencePath("s3a://other/key", false, Option.empty(), Option.apply("1")));
  }

  @Test
  void everySchemeLessPathNeedsItsTableAndStaysInsideIt() {
    assertThrows(IllegalArgumentException.class,
        () -> BatchedBlobReader.resolveReferencePath("rel/a.bin", false, Option.empty(), Option.apply("1")));
    assertThrows(IllegalArgumentException.class,
        () -> BatchedBlobReader.resolveReferencePath("../other.bin", false, Option.apply("s3a://bucket/table"), Option.apply("1")));
  }

  @Test
  void javaCallersRetainTheLegacyThreeArgumentEntryPoint() {
    assertEquals("/abs/a.bin", BatchedBlobReader.resolveReferencePath("/abs/a.bin", true, Option.apply("s3a://bucket/table")));
    assertEquals("rel/a.bin", BatchedBlobReader.resolveReferencePath("rel/a.bin", false, Option.empty()));
  }
}
