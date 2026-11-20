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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Resolves configured file preprocessors by extension. The shipped defaults: PDF for {@code pdf}, {@link ImageInputPreprocessor}
 * for the {@link #IMAGE_EXTENSIONS}, and the video preprocessor of the job, {@link EmbeddingTableConfig#PREP_VIDEO_PREPROCESSOR_CLASS}
 * (default {@link VideoFramePreprocessor}, FFmpeg loaded lazily), for every one of the {@link #VIDEO_EXTENSIONS}. A class
 * configured for an extension replaces its default and wins over the video key.
 */
public class PreprocessorRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(PreprocessorRegistry.class);

  /** Image extensions mapped to {@link ImageInputPreprocessor} by default; WebP is not one (the JDK cannot decode it). */
  public static final List<String> IMAGE_EXTENSIONS =
      Collections.unmodifiableList(Arrays.asList("png", "jpg", "jpeg", "gif", "bmp", "tif", "tiff"));
  /** Video extensions, mapped to {@link EmbeddingTableConfig#PREP_VIDEO_PREPROCESSOR_CLASS}. */
  public static final List<String> VIDEO_EXTENSIONS =
      Collections.unmodifiableList(Arrays.asList("mp4", "mov", "mkv", "webm", "avi"));
  /** The shipped PDF preprocessor, by name: the prep package does not depend on its pdf sub-package. */
  public static final String PDF_PREPROCESSOR_CLASS = "org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfFilePreprocessor";
  private static final Map<String, String> DEFAULTS = new HashMap<>();

  static {
    DEFAULTS.put("pdf", PDF_PREPROCESSOR_CLASS);
    for (String extension : IMAGE_EXTENSIONS) {
      DEFAULTS.put(extension, ImageInputPreprocessor.class.getName());
    }
  }

  private final Map<String, String> classByExtension = new HashMap<>(DEFAULTS);
  private final Map<String, FilePreprocessor> instances = new HashMap<>();
  /** The class named by {@link EmbeddingTableConfig#PREP_VIDEO_PREPROCESSOR_CLASS}, or its default. */
  private final String videoClass;

  public PreprocessorRegistry(TypedProperties props) {
    String video = ConfigUtils.getStringWithAltKeys(props, EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS,
        EmbeddingTableConfig.DEFAULT_PREP_VIDEO_PREPROCESSOR_CLASS);
    videoClass = video == null || video.trim().isEmpty() ? EmbeddingTableConfig.DEFAULT_PREP_VIDEO_PREPROCESSOR_CLASS : video.trim();
    for (String extension : VIDEO_EXTENSIONS) {
      classByExtension.put(extension, videoClass);
    }
    String prefix = EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX;
    String suffix = EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX;
    for (String key : props.stringPropertyNames()) {
      if (EmbeddingTableConfig.isPreprocessorClassKey(key)) {
        String extension = key.substring(prefix.length(), key.length() - suffix.length()).toLowerCase(Locale.ROOT);
        classByExtension.put(extension, ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(key).noDefaultValue()));
      }
    }
  }

  public Option<FilePreprocessor> forExtension(String extension) {
    if (extension == null) {
      return Option.empty();
    }
    String className = classByExtension.get(extension.toLowerCase(Locale.ROOT));
    if (className == null) {
      return Option.empty();
    }
    return Option.of(instances.computeIfAbsent(className, ReflectionUtils::loadClass));
  }

  /** Every extension this registry maps to a preprocessor class, lower case. */
  public Set<String> extensions() {
    return Collections.unmodifiableSet(classByExtension.keySet());
  }

  /**
   * Closes every preprocessor this registry resolved that is {@link AutoCloseable} (one holding per-task resources), once, quietly: a failing close is logged and never hides the outcome it
   * follows. The prep framework calls it when its task's iterator closes.
   */
  public void close() {
    for (FilePreprocessor preprocessor : instances.values()) {
      if (preprocessor instanceof AutoCloseable) {
        try {
          ((AutoCloseable) preprocessor).close();
        } catch (Exception e) {
          LOG.warn("Closing preprocessor {} failed", preprocessor.name(), e);
        }
      }
    }
    instances.clear();
  }

  /**
   * Validates the configuration of the preprocessor of every extension this registry resolves (driver side), and that the video
   * key names a preprocessor that reads videos ({@link SourceKind#VIDEO}). The default video preprocessor's check never loads
   * FFmpeg ({@link VideoFramePreprocessor#validateConfig}).
   */
  public void validateConfig(TypedProperties props, Tokenizer tokenizer) {
    SourceKind videoKind = instances.computeIfAbsent(videoClass, ReflectionUtils::loadClass).sourceKind();
    if (videoKind != SourceKind.VIDEO) {
      throw new IllegalArgumentException("Config " + EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key() + " = " + videoClass
          + " is a " + videoKind + " preprocessor; it must name a video preprocessor (e.g. "
          + EmbeddingTableConfig.DEFAULT_PREP_VIDEO_PREPROCESSOR_CLASS + ")");
    }
    for (String extension : classByExtension.keySet()) {
      forExtension(extension).get().validateConfig(props, tokenizer);
    }
  }

}
