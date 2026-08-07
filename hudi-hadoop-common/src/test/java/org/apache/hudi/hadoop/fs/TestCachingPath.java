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

package org.apache.hudi.hadoop.fs;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestCachingPath {

  @Test
  void testCachesDerivedValues() {
    CachingPath path = new CachingPath("s3://bucket/table/partition/file.parquet");

    String name = path.getName();
    Path parent = path.getParent();
    String fullPath = path.toString();

    assertSame(name, path.getName());
    assertSame(parent, path.getParent());
    assertSame(fullPath, path.toString());
    assertEquals("file.parquet", name);
    assertEquals("s3://bucket/table/partition", parent.toString());
  }

  @Test
  void testEveryConstructorCreatesEquivalentPath() {
    Path parent = new Path("file:///tmp/table");
    Path child = new Path("partition/file.parquet");
    URI expected = URI.create("file:/tmp/table/partition/file.parquet");

    assertEquals(expected, new CachingPath(parent.toString(), child.toString()).toUri());
    assertEquals(expected, new CachingPath(parent, child.toString()).toUri());
    assertEquals(expected, new CachingPath(parent.toString(), child).toUri());
    assertEquals(expected, new CachingPath(parent, child).toUri());
    assertEquals(expected, new CachingPath(expected).toUri());
  }

  @Test
  void testWrapAndSubPath() {
    CachingPath cached = new CachingPath("s3://bucket/table");
    assertSame(cached, CachingPath.wrap(cached));

    CachingPath wrapped = CachingPath.wrap(new Path("s3://bucket/table"));
    assertEquals(cached, wrapped);
    assertEquals("s3://bucket/table/partition/file.parquet",
        wrapped.subPath("partition/file.parquet").toString());
  }

  @Test
  void testUnsafeConcatenationHandlesSeparatorsAndPreservesUriParts() throws Exception {
    Path base = new Path(new URI("s3", "bucket", "/table", "version=1", "fragment"));

    CachingPath expected = CachingPath.concatPathUnsafe(base, "partition");
    assertEquals("/table/partition", expected.toUri().getPath());
    assertEquals("version=1", expected.toUri().getQuery());
    assertEquals("fragment", expected.toUri().getFragment());

    assertEquals("/table/partition", CachingPath.concatPathUnsafe(new Path("s3://bucket/table/"), "/partition").toUri().getPath());
    assertEquals("/table/partition", CachingPath.concatPathUnsafe(new Path("s3://bucket/table/"), "partition").toUri().getPath());
    assertEquals("/table/partition", CachingPath.concatPathUnsafe(new Path("s3://bucket/table"), "/partition").toUri().getPath());
    assertEquals("/table/partition", CachingPath.concatPathUnsafe(new Path("s3://bucket/table"), new Path("partition")).toUri().getPath());
    assertThrows(IllegalStateException.class,
        () -> CachingPath.concatPathUnsafe(base, new Path("s3://other/absolute")));
  }

  @Test
  void testRelativePathAndSchemeRemoval() {
    CachingPath relative = CachingPath.createRelativePathUnsafe("partition%2Fvalue/file.parquet");
    assertFalse(relative.toUri().isAbsolute());
    assertEquals("partition%2Fvalue/file.parquet", relative.toString());

    Path absolute = new Path("s3://bucket/table/file.parquet");
    Path stripped = CachingPath.getPathWithoutSchemeAndAuthority(absolute);
    assertTrue(stripped instanceof CachingPath);
    assertEquals("/table/file.parquet", stripped.toString());

    Path alreadyRelative = new Path("table/file.parquet");
    assertSame(alreadyRelative, CachingPath.getPathWithoutSchemeAndAuthority(alreadyRelative));
  }
}
