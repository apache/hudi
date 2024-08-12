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

package org.apache.hudi.io.storage;

import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link StoragePath}
 */
public class TestStoragePath {
  @Test
  public void testToString() {
    Arrays.stream(
            new String[] {
                "/",
                "/foo",
                "/foo/bar",
                "foo",
                "foo/bar",
                "/foo/bar#boo",
                "foo/bar#boo",
                "file:/a/b/c",
                "s3://a/b/c"})
        .forEach(this::toStringTest);
  }

  @Test
  public void testNormalize() throws URISyntaxException {
    assertEquals("", new StoragePath(".").toString());
    assertEquals("..", new StoragePath("..").toString());
    assertEquals("/", new StoragePath("/").toString());
    assertEquals("/", new StoragePath("//").toString());
    assertEquals("/", new StoragePath("///").toString());
    assertEquals("//foo/", new StoragePath("//foo/").toString());
    assertEquals("//foo/", new StoragePath("//foo//").toString());
    assertEquals("//foo/bar", new StoragePath("//foo//bar").toString());
    assertEquals("/foo", new StoragePath("/foo/").toString());
    assertEquals("/foo", new StoragePath("/foo/").toString());
    assertEquals("foo", new StoragePath("foo/").toString());
    assertEquals("foo", new StoragePath("foo//").toString());
    assertEquals("foo/bar", new StoragePath("foo//bar").toString());
    assertEquals("file:/a/b/c", new StoragePath("file:///a/b/c").toString());
    assertEquals("s3://a/b/c/d/e", new StoragePath("s3://a/b/c", "d/e").toString());
    assertEquals("s3://a/b/c/d/e", new StoragePath("s3://a/b/c/", "d/e").toString());
    assertEquals("s3://a/b/c/d/e", new StoragePath("s3://a/b/c/", "d/e/").toString());
    assertEquals("s3://a/b/c", new StoragePath("s3://a/b/c/", "/").toString());
    assertEquals("s3://a/b/c", new StoragePath("s3://a/b/c/", "").toString());
    assertEquals("s3://a/b/c/d/e", new StoragePath(new StoragePath("s3://a/b/c"), "d/e").toString());
    assertEquals("s3://a/b/c/d/e", new StoragePath(new StoragePath("s3://a/b/c/"), "d/e").toString());
    assertEquals("s3://a/b/c/d/e", new StoragePath(new StoragePath("s3://a/b/c/"), "d/e/").toString());
    assertEquals("s3://a/b/c", new StoragePath(new StoragePath("s3://a/b/c/"), "/").toString());
    assertEquals("s3://a/b/c", new StoragePath(new StoragePath("s3://a/b/c/"), "").toString());
    assertEquals("hdfs://foo/foo2/bar/baz/", new StoragePath(new URI("hdfs://foo//foo2///bar/baz///")).toString());
  }

  @Test
  public void testIsAbsolute() {
    assertTrue(new StoragePath("/").isAbsolute());
    assertTrue(new StoragePath("/foo").isAbsolute());
    assertFalse(new StoragePath("foo").isAbsolute());
    assertFalse(new StoragePath("foo/bar").isAbsolute());
    assertFalse(new StoragePath(".").isAbsolute());
  }

  @Test
  public void testGetParent() {
    assertEquals(new StoragePath("/foo"), new StoragePath("/foo/bar").getParent());
    assertEquals(new StoragePath("foo"), new StoragePath("foo/bar").getParent());
    assertEquals(new StoragePath("/"), new StoragePath("/foo").getParent());
    assertEquals(new StoragePath("/foo/bar/x"), new StoragePath("/foo/bar", "x/y").getParent());
    assertEquals(new StoragePath("/foo/bar"), new StoragePath("/foo/bar/", "y").getParent());
    assertEquals(new StoragePath("/foo"), new StoragePath("/foo/bar/", "/").getParent());
    assertThrows(IllegalStateException.class, () -> new StoragePath("/").getParent());
  }

  @Test
  public void testURI() throws URISyntaxException {
    URI uri = new URI("file:///bar#baz");
    StoragePath path = new StoragePath(uri);
    assertEquals(uri, new URI(path.toString()));
    assertEquals("foo://bar/baz#boo", new StoragePath("foo://bar/", "/baz#boo").toString());
    assertEquals("foo://bar/baz/fud#boo",
        new StoragePath(new StoragePath(new URI("foo://bar/baz#bud")), "fud#boo").toString());
    assertEquals("foo://bar/fud#boo",
        new StoragePath(new StoragePath(new URI("foo://bar/baz#bud")), "/fud#boo").toString());
  }

  @Test
  public void testEncoded() {
    // encoded character like `%2F` should be kept as is
    assertEquals(new StoragePath("s3://foo/bar/1%2F2%2F3"), new StoragePath("s3://foo/bar", "1%2F2%2F3"));
    assertEquals("s3://foo/bar/1%2F2%2F3", new StoragePath("s3://foo/bar", "1%2F2%2F3").toString());
    assertEquals(new StoragePath("s3://foo/bar/1%2F2%2F3"),
        new StoragePath(new StoragePath("s3://foo/bar"), "1%2F2%2F3"));
    assertEquals("s3://foo/bar/1%2F2%2F3",
        new StoragePath(new StoragePath("s3://foo/bar"), "1%2F2%2F3").toString());
    assertEquals("s3://foo/bar/1%2F2%2F3", new StoragePath("s3://foo/bar/1%2F2%2F3").toString());
  }

  @Test
  public void testPathToUriConversion() throws URISyntaxException {
    assertEquals(new URI(null, null, "/foo?bar", null, null),
        new StoragePath("/foo?bar").toUri());
    assertEquals(new URI(null, null, "/foo\"bar", null, null),
        new StoragePath("/foo\"bar").toUri());
    assertEquals(new URI(null, null, "/foo bar", null, null),
        new StoragePath("/foo bar").toUri());
    assertEquals("/foo?bar", new StoragePath("http://localhost/foo?bar").toUri().getPath());
    assertEquals("/foo", new URI("http://localhost/foo?bar").getPath());
    assertEquals((new URI("/foo;bar")).getPath(), new StoragePath("/foo;bar").toUri().getPath());
    assertEquals(new URI("/foo;bar"), new StoragePath("/foo;bar").toUri());
    assertEquals(new URI("/foo+bar"), new StoragePath("/foo+bar").toUri());
    assertEquals(new URI("/foo-bar"), new StoragePath("/foo-bar").toUri());
    assertEquals(new URI("/foo=bar"), new StoragePath("/foo=bar").toUri());
    assertEquals(new URI("/foo,bar"), new StoragePath("/foo,bar").toUri());
  }

  @Test
  public void testGetName() {
    assertEquals("", new StoragePath("/").getName());
    assertEquals("foo", new StoragePath("foo").getName());
    assertEquals("foo", new StoragePath("/foo").getName());
    assertEquals("foo", new StoragePath("/foo/").getName());
    assertEquals("bar", new StoragePath("/foo/bar").getName());
    assertEquals("bar", new StoragePath("hdfs://host/foo/bar").getName());
    assertEquals("bar", new StoragePath("hdfs://host", "foo/bar").getName());
    assertEquals("bar", new StoragePath("hdfs://host/foo/", "bar").getName());
  }

  @Test
  public void testGetPathWithoutSchemeAndAuthority() {
    assertEquals(
        new StoragePath("/foo/bar/boo"),
        new StoragePath("/foo/bar/boo").getPathWithoutSchemeAndAuthority());
    assertEquals(
        new StoragePath("/foo/bar/boo"),
        new StoragePath("file:///foo/bar/boo").getPathWithoutSchemeAndAuthority());
    assertEquals(
        new StoragePath("/bar/boo"),
        new StoragePath("s3://foo/bar/boo").getPathWithoutSchemeAndAuthority());
  }

  @Test
  public void testDepth() throws URISyntaxException {
    assertEquals(0, new StoragePath("/").depth());
    assertEquals(0, new StoragePath("///").depth());
    assertEquals(0, new StoragePath("//foo/").depth());
    assertEquals(1, new StoragePath("//foo//bar").depth());
    assertEquals(5, new StoragePath("/a/b/c/d/e").depth());
    assertEquals(4, new StoragePath("s3://a/b/c", "d/e").depth());
    assertEquals(2, new StoragePath("s3://a/b/c/", "").depth());
    assertEquals(4, new StoragePath(new StoragePath("s3://a/b/c"), "d/e").depth());
  }

  @Test
  public void testMakeQualified() throws URISyntaxException {
    URI defaultUri = new URI("hdfs://host1/dir1");
    assertEquals(new StoragePath("hdfs://host1/a/b/c"),
        new StoragePath("/a/b/c").makeQualified(defaultUri));
    assertEquals(new StoragePath("hdfs://host2/a/b/c"),
        new StoragePath("hdfs://host2/a/b/c").makeQualified(defaultUri));
    assertEquals(new StoragePath("hdfs://host1/a/b/c"),
        new StoragePath("hdfs:/a/b/c").makeQualified(defaultUri));
    assertEquals(new StoragePath("s3://a/b/c"),
        new StoragePath("s3://a/b/c/").makeQualified(defaultUri));
    assertThrows(IllegalStateException.class,
        () -> new StoragePath("a").makeQualified(defaultUri));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "/x/y/1.file#bar",
      "s3://foo/bar/1%2F2%2F3",
      "hdfs://host1/a/b/c"
  })
  public void testSerializability(String pathStr) throws IOException, ClassNotFoundException {
    StoragePath path = new StoragePath(pathStr);
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(path);
      try (ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
           ObjectInputStream ois = new ObjectInputStream(bais)) {
        StoragePath deserialized = (StoragePath) ois.readObject();
        assertEquals(path.toUri(), deserialized.toUri());
      }
    }
  }

  @Test
  public void testEquals() {
    assertEquals(new StoragePath("/foo"), new StoragePath("/foo"));
    assertEquals(new StoragePath("/foo"), new StoragePath("/foo/"));
    assertEquals(new StoragePath("/foo/bar"), new StoragePath("/foo//bar/"));
    assertNotEquals(new StoragePath("/"), new StoragePath("/foo"));
  }

  @Test
  public void testCachedResults() {
    StoragePath path = new StoragePath("s3://x/y/z/");
    assertSame(path.getParent(), path.getParent());
    assertSame(path.getName(), path.getName());
    assertSame(path.toString(), path.toString());
  }

  private void toStringTest(String pathString) {
    assertEquals(pathString, new StoragePath(pathString).toString());
  }
}
