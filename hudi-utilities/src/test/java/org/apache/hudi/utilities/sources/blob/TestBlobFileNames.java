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

package org.apache.hudi.utilities.sources.blob;

import org.apache.hudi.utilities.sources.unstructured.UnstructuredTableVersion;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestBlobFileNames {

  @Test
  void fidIsDeterministic32Hex() throws Exception {
    String path = "file:///tmp/data/a.pdf";
    String fid = BlobFileNames.sourcePathSha(path);
    assertEquals(fid, BlobFileNames.sourcePathSha(path));
    assertTrue(fid.matches("[0-9a-f]{32}"), fid);
    byte[] digest = MessageDigest.getInstance("SHA-256").digest(path.getBytes(StandardCharsets.UTF_8));
    StringBuilder hex = new StringBuilder();
    for (byte b : digest) {
      hex.append(String.format("%02x", b));
    }
    assertEquals(hex.substring(0, 32), fid);
    assertTrue(!fid.equals(BlobFileNames.sourcePathSha("file:///tmp/data/b.pdf")));
  }

  @Test
  void rawSelectorS3AliasesHaveTheSameKey() {
    assertEquals(BlobFileNames.sourcePathSha("s3://bkt/k/a.pdf"), BlobFileNames.sourcePathSha("s3a://bkt/k/a.pdf"));
    assertEquals(BlobFileNames.sourcePathSha("s3://bkt/k/a.pdf"), BlobFileNames.sourcePathSha("s3n://bkt/k/a.pdf"));
  }

  @Test
  void objectKeyTextIsNotNormalized() {
    for (String key : new String[] {"a//b.pdf", "a/../b.pdf", "a%2Fb.pdf", "a+b.pdf", "A/b.pdf", "a/b.pdf?x#part"}) {
      assertNotEquals(BlobFileNames.sourcePathSha("s3://bkt/a/b.pdf"), BlobFileNames.sourcePathSha("s3://bkt/" + key), key);
    }
    assertNotEquals(BlobFileNames.sourcePathSha("s3://bkt/café.pdf"), BlobFileNames.sourcePathSha("s3://bkt/café.pdf"));
    assertNotEquals(BlobFileNames.sourcePathSha("s3://bkt/key"), BlobFileNames.sourcePathSha("s3://BKT/key"));
  }

  @Test
  void sharedKeyEncodingGoldenVectors() throws Exception {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        TestBlobFileNames.class.getResourceAsStream("/unstructured/source-path-key-v1.tsv"), StandardCharsets.UTF_8))) {
      String line;
      int vectors = 0;
      while ((line = reader.readLine()) != null) {
        if (line.startsWith("#") || line.isEmpty()) {
          continue;
        }
        String[] fields = line.split("\\t", -1);
        assertEquals(3, fields.length, line);
        assertEquals(fields[2], BlobFileNames.sourcePathSha(fields[0]), fields[0]);
        assertEquals(fields[2], BlobFileNames.sourcePathSha(fields[1]), fields[1]);
        vectors++;
      }
      assertEquals(19, vectors);
    }
  }

  @Test
  void canonicalPathOfLocalFile() {
    assertEquals("file:///tmp/a.pdf", BlobTestPaths.canonicalPath(new File("/tmp/x/../a.pdf").toURI()));
    assertEquals("file:///tmp/a.pdf", BlobTestPaths.canonicalPath(URI.create("file:/tmp/a.pdf")));
  }

  @Test
  void canonicalPathOfS3() {
    assertEquals("s3a://bkt/a/b.pdf", BlobTestPaths.canonicalPath(URI.create("s3a://bkt/a//b.pdf")));
  }

  @Test
  void canonicalPathIsDecoded() {
    // source_path is the decoded form Hadoop's Path prints and StoragePath(String) opens; file_name is its last segment
    String canonical = BlobTestPaths.canonicalPath(new File("/tmp/my scan%.pdf").toURI());
    assertEquals("file:///tmp/my scan%.pdf", canonical);
    assertEquals("my scan%.pdf", BlobFileNames.fileName(canonical));
    assertEquals("file:///tmp/my scan%.pdf", BlobTestPaths.canonicalPath(URI.create("file:/tmp/my%20scan%25.pdf")));
  }

  @Test
  void s3SchemesShareOneCanonicalForm() {
    assertEquals("s3a://bkt/k/a.pdf", BlobTestPaths.canonicalPath(URI.create("s3://bkt/k/a.pdf")));
    assertEquals("s3a://bkt/k/a.pdf", BlobTestPaths.canonicalPath(URI.create("s3n://bkt/k/a.pdf")));
    assertEquals(BlobFileNames.sourcePathSha(BlobTestPaths.canonicalPath(URI.create("s3://bkt/k/a.pdf"))),
        BlobFileNames.sourcePathSha(BlobTestPaths.canonicalPath(URI.create("s3a://bkt/k/a.pdf"))));
  }

  @Test
  void opaqueOrRelativeUriIsRejected() {
    URI opaque = URI.create("mailto:a@b.c");
    URI relative = URI.create("docs/a.pdf");
    assertThrows(IllegalArgumentException.class, () -> BlobTestPaths.canonicalPath(opaque));
    assertThrows(IllegalArgumentException.class, () -> BlobTestPaths.canonicalPath(relative));
  }

  @Test
  void extensionIsLowercased() {
    assertEquals("pdf", BlobFileNames.extension("SCAN.PDF"));
    assertEquals("gz", BlobFileNames.extension("a.tar.gz"));
    assertEquals("", BlobFileNames.extension("README"));
    assertEquals("b.pdf", BlobFileNames.fileName("file:///tmp/a/b.pdf"));
  }

  @Test
  void contentTypeOfPdf() {
    assertEquals("application/pdf", BlobFileNames.contentType("pdf"));
  }

  @Test
  void contentTypesOfImages() {
    assertEquals("image/png", BlobFileNames.contentType("png"));
    assertEquals("image/jpeg", BlobFileNames.contentType("jpg"));
    assertEquals("image/jpeg", BlobFileNames.contentType("jpeg"));
    assertEquals("image/gif", BlobFileNames.contentType("gif"));
    assertEquals("image/bmp", BlobFileNames.contentType("bmp"));
    assertEquals("image/tiff", BlobFileNames.contentType("tif"));
    assertEquals("image/tiff", BlobFileNames.contentType("tiff"));
    assertEquals("image/webp", BlobFileNames.contentType("webp"));
    assertNull(BlobFileNames.contentType("mp4"), "video types are not named here");
  }

  /** The extension-to-MIME map of blob layout version 1, whole: a change to it needs a new blob layout version. */
  @Test
  void contentTypesAreFrozenForBlobLayoutVersion1() {
    Map<String, String> layoutVersion1 = new HashMap<>();
    layoutVersion1.put("pdf", "application/pdf");
    layoutVersion1.put("png", "image/png");
    layoutVersion1.put("jpg", "image/jpeg");
    layoutVersion1.put("jpeg", "image/jpeg");
    layoutVersion1.put("gif", "image/gif");
    layoutVersion1.put("bmp", "image/bmp");
    layoutVersion1.put("tif", "image/tiff");
    layoutVersion1.put("tiff", "image/tiff");
    layoutVersion1.put("webp", "image/webp");
    assertEquals("1", UnstructuredTableVersion.tablePropertiesBuilder("fixture", "blob").build().tableProperties()
        .getProperty(UnstructuredTableVersion.BLOB_LAYOUT_VERSION.key()), "a new layout version may have its own map");
    assertEquals(layoutVersion1, BlobFileNames.CONTENT_TYPES);
    assertThrows(UnsupportedOperationException.class, () -> BlobFileNames.CONTENT_TYPES.put("mp4", "video/mp4"));
    assertNull(BlobFileNames.contentType("mp4"), "a video row has no content type in layout version 1");
  }
}
