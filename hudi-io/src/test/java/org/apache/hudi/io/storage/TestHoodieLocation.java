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

import org.junit.jupiter.api.Test;

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
 * Tests {@link HoodieLocation}
 */
public class TestHoodieLocation {
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
    assertEquals("", new HoodieLocation(".").toString());
    assertEquals("..", new HoodieLocation("..").toString());
    assertEquals("/", new HoodieLocation("/").toString());
    assertEquals("/", new HoodieLocation("//").toString());
    assertEquals("/", new HoodieLocation("///").toString());
    assertEquals("//foo/", new HoodieLocation("//foo/").toString());
    assertEquals("//foo/", new HoodieLocation("//foo//").toString());
    assertEquals("//foo/bar", new HoodieLocation("//foo//bar").toString());
    assertEquals("/foo", new HoodieLocation("/foo/").toString());
    assertEquals("/foo", new HoodieLocation("/foo/").toString());
    assertEquals("foo", new HoodieLocation("foo/").toString());
    assertEquals("foo", new HoodieLocation("foo//").toString());
    assertEquals("foo/bar", new HoodieLocation("foo//bar").toString());
    assertEquals("file:/a/b/c", new HoodieLocation("file:///a/b/c").toString());
    assertEquals("s3://a/b/c/d/e", new HoodieLocation("s3://a/b/c", "d/e").toString());
    assertEquals("s3://a/b/c/d/e", new HoodieLocation("s3://a/b/c/", "d/e").toString());
    assertEquals("s3://a/b/c/d/e", new HoodieLocation("s3://a/b/c/", "d/e/").toString());
    assertEquals("s3://a/b/c", new HoodieLocation("s3://a/b/c/", "/").toString());
    assertEquals("s3://a/b/c", new HoodieLocation("s3://a/b/c/", "").toString());
    assertEquals("s3://a/b/c/d/e", new HoodieLocation(new HoodieLocation("s3://a/b/c"), "d/e").toString());
    assertEquals("s3://a/b/c/d/e", new HoodieLocation(new HoodieLocation("s3://a/b/c/"), "d/e").toString());
    assertEquals("s3://a/b/c/d/e", new HoodieLocation(new HoodieLocation("s3://a/b/c/"), "d/e/").toString());
    assertEquals("s3://a/b/c", new HoodieLocation(new HoodieLocation("s3://a/b/c/"), "/").toString());
    assertEquals("s3://a/b/c", new HoodieLocation(new HoodieLocation("s3://a/b/c/"), "").toString());
    assertEquals("hdfs://foo/foo2/bar/baz/", new HoodieLocation(new URI("hdfs://foo//foo2///bar/baz///")).toString());
  }

  @Test
  public void testIsAbsolute() {
    assertTrue(new HoodieLocation("/").isAbsolute());
    assertTrue(new HoodieLocation("/foo").isAbsolute());
    assertFalse(new HoodieLocation("foo").isAbsolute());
    assertFalse(new HoodieLocation("foo/bar").isAbsolute());
    assertFalse(new HoodieLocation(".").isAbsolute());
  }

  @Test
  public void testGetParent() {
    assertEquals(new HoodieLocation("/foo"), new HoodieLocation("/foo/bar").getParent());
    assertEquals(new HoodieLocation("foo"), new HoodieLocation("foo/bar").getParent());
    assertEquals(new HoodieLocation("/"), new HoodieLocation("/foo").getParent());
    assertEquals(new HoodieLocation("/foo/bar/x"), new HoodieLocation("/foo/bar", "x/y").getParent());
    assertEquals(new HoodieLocation("/foo/bar"), new HoodieLocation("/foo/bar/", "y").getParent());
    assertEquals(new HoodieLocation("/foo"), new HoodieLocation("/foo/bar/", "/").getParent());
    assertThrows(IllegalStateException.class, () -> new HoodieLocation("/").getParent());
  }

  @Test
  public void testURI() throws URISyntaxException {
    URI uri = new URI("file:///bar#baz");
    HoodieLocation location = new HoodieLocation(uri);
    assertEquals(uri, new URI(location.toString()));
    assertEquals("foo://bar/baz#boo", new HoodieLocation("foo://bar/", "/baz#boo").toString());
    assertEquals("foo://bar/baz/fud#boo",
        new HoodieLocation(new HoodieLocation(new URI("foo://bar/baz#bud")), "fud#boo").toString());
    assertEquals("foo://bar/fud#boo",
        new HoodieLocation(new HoodieLocation(new URI("foo://bar/baz#bud")), "/fud#boo").toString());
  }

  @Test
  public void testPathToUriConversion() throws URISyntaxException {
    assertEquals(new URI(null, null, "/foo?bar", null, null),
        new HoodieLocation("/foo?bar").toUri());
    assertEquals(new URI(null, null, "/foo\"bar", null, null),
        new HoodieLocation("/foo\"bar").toUri());
    assertEquals(new URI(null, null, "/foo bar", null, null),
        new HoodieLocation("/foo bar").toUri());
    assertEquals("/foo?bar", new HoodieLocation("http://localhost/foo?bar").toUri().getPath());
    assertEquals("/foo", new URI("http://localhost/foo?bar").getPath());
    assertEquals((new URI("/foo;bar")).getPath(), new HoodieLocation("/foo;bar").toUri().getPath());
    assertEquals(new URI("/foo;bar"), new HoodieLocation("/foo;bar").toUri());
    assertEquals(new URI("/foo+bar"), new HoodieLocation("/foo+bar").toUri());
    assertEquals(new URI("/foo-bar"), new HoodieLocation("/foo-bar").toUri());
    assertEquals(new URI("/foo=bar"), new HoodieLocation("/foo=bar").toUri());
    assertEquals(new URI("/foo,bar"), new HoodieLocation("/foo,bar").toUri());
  }

  @Test
  public void testGetName() {
    assertEquals("", new HoodieLocation("/").getName());
    assertEquals("foo", new HoodieLocation("foo").getName());
    assertEquals("foo", new HoodieLocation("/foo").getName());
    assertEquals("foo", new HoodieLocation("/foo/").getName());
    assertEquals("bar", new HoodieLocation("/foo/bar").getName());
    assertEquals("bar", new HoodieLocation("hdfs://host/foo/bar").getName());
    assertEquals("bar", new HoodieLocation("hdfs://host", "foo/bar").getName());
    assertEquals("bar", new HoodieLocation("hdfs://host/foo/", "bar").getName());
  }

  @Test
  public void testGetLocationWithoutSchemeAndAuthority() {
    assertEquals(
        new HoodieLocation("/foo/bar/boo"),
        new HoodieLocation("/foo/bar/boo").getLocationWithoutSchemeAndAuthority());
    assertEquals(
        new HoodieLocation("/foo/bar/boo"),
        new HoodieLocation("file:///foo/bar/boo").getLocationWithoutSchemeAndAuthority());
    assertEquals(
        new HoodieLocation("/bar/boo"),
        new HoodieLocation("s3://foo/bar/boo").getLocationWithoutSchemeAndAuthority());
  }

  @Test
  public void testDepth() throws URISyntaxException {
    assertEquals(0, new HoodieLocation("/").depth());
    assertEquals(0, new HoodieLocation("///").depth());
    assertEquals(0, new HoodieLocation("//foo/").depth());
    assertEquals(1, new HoodieLocation("//foo//bar").depth());
    assertEquals(5, new HoodieLocation("/a/b/c/d/e").depth());
    assertEquals(4, new HoodieLocation("s3://a/b/c", "d/e").depth());
    assertEquals(2, new HoodieLocation("s3://a/b/c/", "").depth());
    assertEquals(4, new HoodieLocation(new HoodieLocation("s3://a/b/c"), "d/e").depth());
  }

  @Test
  public void testEquals() {
    assertEquals(new HoodieLocation("/foo"), new HoodieLocation("/foo"));
    assertEquals(new HoodieLocation("/foo"), new HoodieLocation("/foo/"));
    assertEquals(new HoodieLocation("/foo/bar"), new HoodieLocation("/foo//bar/"));
    assertNotEquals(new HoodieLocation("/"), new HoodieLocation("/foo"));
  }

  @Test
  public void testCachedResults() {
    HoodieLocation location = new HoodieLocation("s3://x/y/z/");
    assertSame(location.getParent(), location.getParent());
    assertSame(location.getName(), location.getName());
    assertSame(location.toString(), location.toString());
  }

  private void toStringTest(String pathString) {
    assertEquals(pathString, new HoodieLocation(pathString).toString());
  }
}
