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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link PreprocessorRegistry}: a file extension resolves to the preprocessor class its
 * {@code hoodie.unstructured.embedding.prep.preprocessor.<ext>.class} key names, one instance per class, closed with the registry.
 */
public class TestPreprocessorRegistry {

  /** A preprocessor holding a per-task resource. */
  public static class CloseablePreprocessor implements FilePreprocessor, AutoCloseable {
    static final AtomicInteger CLOSED = new AtomicInteger();

    @Override
    public String name() {
      return "closeable-v1";
    }

    @Override
    public SourceKind sourceKind() {
      return SourceKind.DOCUMENT;
    }

    @Override
    public ClosableIterator<Chunk> open(BlobBytesSupplier bytes, FileMeta file, Tokenizer tokenizer, TypedProperties props) {
      return ClosableIterator.wrap(Collections.<Chunk>emptyIterator());
    }

    @Override
    public void close() {
      CLOSED.incrementAndGet();
    }
  }

  /** A preprocessor whose close fails. */
  public static class FailingClosePreprocessor extends CloseablePreprocessor {
    @Override
    public void close() {
      super.close();
      throw new IllegalStateException("close failed");
    }
  }

  @BeforeEach
  public void reset() {
    CloseablePreprocessor.CLOSED.set(0);
  }

  private static String key(String extension) {
    return EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + extension + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX;
  }

  private static TypedProperties props(String... extensionAndClass) {
    TypedProperties props = new TypedProperties();
    for (int i = 0; i < extensionAndClass.length; i += 2) {
      props.setProperty(key(extensionAndClass[i]), extensionAndClass[i + 1]);
    }
    return props;
  }

  @Test
  public void resolvesTheConfiguredClassIgnoringCase() {
    PreprocessorRegistry registry = new PreprocessorRegistry(props("PDF", CloseablePreprocessor.class.getName()));

    assertInstanceOf(CloseablePreprocessor.class, registry.forExtension("pdf").get());
    assertInstanceOf(CloseablePreprocessor.class, registry.forExtension("Pdf").get());
  }

  @Test
  public void anUnconfiguredOrMissingExtensionHasNoPreprocessor() {
    PreprocessorRegistry registry = new PreprocessorRegistry(props("pdf", CloseablePreprocessor.class.getName()));

    assertFalse(registry.forExtension("txt").isPresent());
    assertFalse(registry.forExtension(null).isPresent());
    assertFalse(new PreprocessorRegistry(new TypedProperties()).forExtension("txt").isPresent(), "txt has no default");
  }

  @Test
  public void otherKeysUnderThePrefixAreNotExtensions() {
    TypedProperties props = props("pdf", CloseablePreprocessor.class.getName());
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "pdf.max.pages", "3");

    PreprocessorRegistry registry = new PreprocessorRegistry(props);

    assertFalse(registry.forExtension("pdf.max.pages").isPresent());
    assertInstanceOf(CloseablePreprocessor.class, registry.forExtension("pdf").get());
  }

  @Test
  public void extensionsNamingOneClassShareOneInstance() {
    PreprocessorRegistry registry = new PreprocessorRegistry(props(
        "pdf", CloseablePreprocessor.class.getName(), "pdfa", CloseablePreprocessor.class.getName()));

    assertSame(registry.forExtension("pdf").get(), registry.forExtension("pdfa").get());
    assertNotSame(registry.forExtension("pdf").get(),
        new PreprocessorRegistry(props("pdf", CloseablePreprocessor.class.getName())).forExtension("pdf").get());
  }

  @Test
  public void closeClosesEveryResolvedCloseablePreprocessorOnce() {
    PreprocessorRegistry registry = new PreprocessorRegistry(props(
        "pdf", CloseablePreprocessor.class.getName(), "doc", FailingClosePreprocessor.class.getName()));
    registry.forExtension("pdf");
    registry.forExtension("doc");

    registry.close();
    registry.close();

    assertEquals(2, CloseablePreprocessor.CLOSED.get(), "a failing close neither hides the other nor repeats");
  }

  @Test
  public void anUnresolvedPreprocessorIsNotCreatedToBeClosed() {
    PreprocessorRegistry registry = new PreprocessorRegistry(props("pdf", CloseablePreprocessor.class.getName()));

    registry.close();

    assertEquals(0, CloseablePreprocessor.CLOSED.get());
  }

  /**
   * Unset, the video key maps every video extension to the FFmpeg {@link VideoFramePreprocessor}; validating a job with the
   * default configuration (a PDF-only job included) checks it without loading FFmpeg (the class-graph proof, with the FFmpeg jars
   * hidden, is {@code TestFfmpegAbsentClassGraph}).
   */
  @Test
  public void videoExtensionsMapToTheFramePreprocessorByDefault() {
    TypedProperties props = new TypedProperties();
    PreprocessorRegistry registry = new PreprocessorRegistry(props);

    assertEquals(VideoFramePreprocessor.class.getName(), EmbeddingTableConfig.DEFAULT_PREP_VIDEO_PREPROCESSOR_CLASS);
    for (String extension : PreprocessorRegistry.VIDEO_EXTENSIONS) {
      assertEquals(VideoFramePreprocessor.class, registry.forExtension(extension).get().getClass(), extension);
    }
    assertSame(registry.forExtension("mp4").get(), registry.forExtension("avi").get(), "one video preprocessor per job");
    registry.validateConfig(props, new WordCountTokenizer());
  }

  @Test
  public void anExtensionKeyWinsOverTheVideoKeyForItsExtension() {
    TypedProperties props = props("mov", FailingClosePreprocessor.class.getName());
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key(), CloseablePreprocessor.class.getName());
    PreprocessorRegistry registry = new PreprocessorRegistry(props);

    assertInstanceOf(FailingClosePreprocessor.class, registry.forExtension("mov").get());
    assertEquals(CloseablePreprocessor.class, registry.forExtension("mp4").get().getClass());
  }

  @Test
  public void theVideoKeyMustNameAVideoPreprocessor() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key(), CloseablePreprocessor.class.getName());

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> new PreprocessorRegistry(props).validateConfig(props, null));
    assertTrue(e.getMessage().contains(EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key() + " = "
        + CloseablePreprocessor.class.getName() + " is a DOCUMENT preprocessor; it must name a video preprocessor"), e.getMessage());
  }
}
