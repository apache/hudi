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

package org.apache.hudi.utilities.unstructured.embedding.image;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.prep.Chunk;
import org.apache.hudi.utilities.unstructured.embedding.prep.ImageInputPreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.imageio.stream.FileImageInputStream;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The generated images of {@link ImageFormatFixtures}: every small one decodes as its declared format and size, its header reads as
 * that format, and image prep stores it as one image row; a regeneration is byte-identical. (The near-bound images need a heap of
 * a few GiB to generate and are not written here.)
 */
public class TestImageFormatFixtures {

  @TempDir
  Path tempDir;

  @Test
  public void smallFixturesDecodeAndArePrepared() throws IOException {
    int small = 0;
    for (ImageFormatFixtures.Fixture fixture : ImageFormatFixtures.catalog()) {
      if (fixture.isNearBound()) {
        continue;
      }
      small++;
      Path file = ImageFormatFixtures.write(fixture, tempDir);
      byte[] bytes = Files.readAllBytes(file);
      BufferedImage decoded = ImageFixtures.decode(bytes);
      assertEquals(fixture.width(), decoded.getWidth(), fixture.name());
      assertEquals(fixture.height(), decoded.getHeight(), fixture.name());
      try (FileImageInputStream in = new FileImageInputStream(file.toFile())) {
        ImageHeader header = ImageHeaderReader.read(in);
        assertEquals(fixture.format(), header.getFormat(), fixture.name());
        assertEquals(fixture.width(), header.getWidth(), fixture.name());
        assertEquals(fixture.height(), header.getHeight(), fixture.name());
      }
      String extension = fixture.name().substring(fixture.name().lastIndexOf('.') + 1);
      List<Chunk> chunks = new ArrayList<>();
      try (ClosableIterator<Chunk> it = new ImageInputPreprocessor().open(() -> new ByteArrayInputStream(bytes),
          new FileMeta("sha-" + fixture.name(), extension, bytes.length), null, new TypedProperties())) {
        it.forEachRemaining(chunks::add);
      }
      assertEquals(1, chunks.size(), fixture.name());
      Map<String, String> expectedAux = new HashMap<>();
      expectedAux.put(ImageInputPreprocessor.AUX_WIDTH, String.valueOf(fixture.width()));
      expectedAux.put(ImageInputPreprocessor.AUX_HEIGHT, String.valueOf(fixture.height()));
      expectedAux.put(ImageInputPreprocessor.AUX_FORMAT, fixture.format());
      expectedAux.put(EmbeddingTableSchema.AUX_INPUT_WIDTH, String.valueOf(fixture.width()));
      expectedAux.put(EmbeddingTableSchema.AUX_INPUT_HEIGHT, String.valueOf(fixture.height()));
      expectedAux.put(EmbeddingTableSchema.AUX_INPUT_LOGIC, ImageInputPreprocessor.INPUT_LOGIC);
      assertEquals(expectedAux, chunks.get(0).getAux(), fixture.name());
      assertTrue(chunks.get(0).isPayloadSource(), fixture.name());
      assertEquals(bytes.length, chunks.get(0).getPayloadSizeBytes(), fixture.name());
      assertEquals("image/" + fixture.format(), chunks.get(0).getPayloadContentType(), fixture.name());
    }
    assertEquals(8, small, "every small fixture of the catalog is checked");
  }

  @Test
  public void regenerationIsByteIdentical() throws IOException {
    for (ImageFormatFixtures.Fixture fixture : ImageFormatFixtures.catalog()) {
      if (fixture.isNearBound()) {
        continue;
      }
      Path first = ImageFormatFixtures.write(fixture, Files.createDirectories(tempDir.resolve("a")));
      Path second = ImageFormatFixtures.write(fixture, Files.createDirectories(tempDir.resolve("b")));
      assertEquals(ImageFormatFixtures.sha256(first), ImageFormatFixtures.sha256(second), fixture.name());
    }
  }
}
