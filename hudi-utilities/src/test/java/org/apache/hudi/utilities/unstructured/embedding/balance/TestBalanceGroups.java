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

package org.apache.hudi.utilities.unstructured.embedding.balance;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessorRegistry;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Grouping prep rows by the preprocessor their extension resolves to ({@link BalanceGroups}): stable group names for the
 * shipped preprocessors, a custom class as its own group, case-insensitive, null and unknown extensions unmapped. The Spark
 * form of the same mapping is exercised by strategy tests.
 */
class TestBalanceGroups {

  @Test
  void defaultsMapPdfImageAndVideo() {
    BalanceGroups groups = BalanceGroups.from(new TypedProperties());
    assertEquals(BalanceGroups.PDF, groups.groupOf("pdf"));
    for (String extension : PreprocessorRegistry.IMAGE_EXTENSIONS) {
      assertEquals(BalanceGroups.IMAGE, groups.groupOf(extension), extension);
    }
    for (String extension : PreprocessorRegistry.VIDEO_EXTENSIONS) {
      assertEquals(BalanceGroups.VIDEO, groups.groupOf(extension), extension);
    }
    assertEquals(new HashSet<>(Arrays.asList(BalanceGroups.PDF, BalanceGroups.IMAGE, BalanceGroups.VIDEO)),
        groups.configuredGroups());
    assertEquals(1 + PreprocessorRegistry.IMAGE_EXTENSIONS.size() + PreprocessorRegistry.VIDEO_EXTENSIONS.size(),
        groups.byExtension().size());
  }

  @Test
  void caseAndNullAndUnknown() {
    BalanceGroups groups = BalanceGroups.from(new TypedProperties());
    assertEquals(BalanceGroups.PDF, groups.groupOf("PDF"));
    assertEquals(BalanceGroups.VIDEO, groups.groupOf("Mp4"));
    assertEquals(BalanceGroups.UNMAPPED, groups.groupOf(null));
    assertEquals(BalanceGroups.UNMAPPED, groups.groupOf("docx"));
    assertEquals(BalanceGroups.UNMAPPED, groups.groupOf(""));
  }

  @Test
  void jcodecVideoClassIsStillVideo() {
    TypedProperties props = new TypedProperties();
    for (String extension : Arrays.asList("mp4", "mov")) {
      props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + extension + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
          "org.apache.hudi.utilities.unstructured.embedding.prep.JcodecVideoFramePreprocessor");
    }
    BalanceGroups groups = BalanceGroups.from(props);
    assertEquals(BalanceGroups.VIDEO, groups.groupOf("mp4"));
    assertEquals(BalanceGroups.VIDEO, groups.groupOf("mov"));
  }

  @Test
  void customClassIsItsOwnGroup() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key(), "com.example.CustomVideo");
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "txt" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        "com.example.TxtPreprocessor");
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "png" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        "com.example.MyImage");
    BalanceGroups groups = BalanceGroups.from(props);
    assertEquals(BalanceGroups.VIDEO, groups.groupOf("mp4"));
    assertEquals("com.example.TxtPreprocessor", groups.groupOf("txt"));
    assertEquals("com.example.MyImage", groups.groupOf("png"));
    assertEquals(BalanceGroups.IMAGE, groups.groupOf("jpg"));
    assertEquals(new HashSet<>(Arrays.asList(BalanceGroups.PDF, BalanceGroups.IMAGE, BalanceGroups.VIDEO,
        "com.example.TxtPreprocessor", "com.example.MyImage")), groups.configuredGroups());
  }

  @Test
  void shippedPreprocessorUnderAnotherExtensionKeepsItsGroup() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "heic" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        "org.apache.hudi.utilities.unstructured.embedding.prep.ImageInputPreprocessor");
    assertEquals(BalanceGroups.IMAGE, BalanceGroups.from(props).groupOf("HEIC"));
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key(), "com.example.CustomVideo");
    register(props, "customvideo", "com.example.CustomVideo");
    assertEquals(BalanceGroups.VIDEO, BalanceGroups.from(props).groupOf("CUSTOMVIDEO"));
  }

  @Test
  void registryDiscoveryReturnsASortedIndependentCopyWithoutLoadingClasses() {
    TypedProperties props = new TypedProperties();
    register(props, "ZZ", "com.example.NotInstalledPreprocessor");
    PreprocessorRegistry registry = new PreprocessorRegistry(props);
    TreeSet<String> expected = new TreeSet<>(PreprocessorRegistry.IMAGE_EXTENSIONS);
    expected.addAll(PreprocessorRegistry.VIDEO_EXTENSIONS);
    expected.addAll(Arrays.asList("pdf", "zz"));
    assertEquals(new ArrayList<>(expected), new ArrayList<>(registry.classNamesByExtension().keySet()));
    registry.classNamesByExtension().clear();
    assertEquals("com.example.NotInstalledPreprocessor", registry.classNamesByExtension().get("zz"));
    registry.close();
  }

  @Test
  void ffmpegVideoClassKeepsItsAliasWithoutBeingLoaded() {
    TypedProperties props = new TypedProperties();
    register(props, "video", "org.apache.hudi.utilities.unstructured.embedding.prep.VideoFramePreprocessor");
    assertEquals(BalanceGroups.VIDEO, BalanceGroups.from(props).groupOf("video"));
  }

  private static void register(TypedProperties props, String extension, String className) {
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + extension + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX, className);
  }
}
