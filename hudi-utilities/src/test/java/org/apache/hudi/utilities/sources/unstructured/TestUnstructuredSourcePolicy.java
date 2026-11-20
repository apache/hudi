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

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.config.CloudSourceConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestUnstructuredSourcePolicy {
  private static final String BLOB_EXTENSIONS = "hoodie.blob.table.file.extensions";

  @Test
  void defaultsToCurrentPdfImageAndVideoTypes() {
    List<String> expected = Arrays.asList("pdf", "png", "jpg", "jpeg", "gif", "bmp", "tif", "tiff", "mp4", "mov", "avi", "mkv", "webm");
    assertEquals(expected, UnstructuredSourcePolicy.fileExtensions(new TypedProperties()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"unknown", "txt", "pdf,txt", "pdf,parquet", "JPG"})
  void refusesStructuredUnknownAndWiderLists(String extensions) {
    TypedProperties props = new TypedProperties();
    props.setProperty(BLOB_EXTENSIONS, extensions);
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> UnstructuredSourcePolicy.fileExtensions(props));
    assertTrue(failure.getMessage().contains(extensions.split(",")[extensions.split(",").length - 1]), failure.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "json", "unstructured", " "})
  void refusesAnExplicitDatafileFormat(String format) {
    TypedProperties props = new TypedProperties();
    props.setProperty(CloudSourceConfig.DATAFILE_FORMAT.key(), format);
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> UnstructuredSourcePolicy.fileExtensions(props));
    assertTrue(failure.getMessage().contains(CloudSourceConfig.DATAFILE_FORMAT.key()), failure.getMessage());
  }

  @Test
  void honoursNarrowingFromBothExtensionConfigs() {
    TypedProperties props = new TypedProperties();
    props.setProperty(BLOB_EXTENSIONS, "pdf,png,jpg");
    props.setProperty(CloudSourceConfig.CLOUD_DATAFILE_EXTENSION.key(), " png, pdf ");
    assertEquals(Arrays.asList("pdf", "png"), UnstructuredSourcePolicy.fileExtensions(props));
  }

  @Test
  void cloudExtensionListAcceptsALeadingDot() {
    TypedProperties props = new TypedProperties();
    props.setProperty(CloudSourceConfig.CLOUD_DATAFILE_EXTENSION.key(), ".pdf, .png,jpg");
    assertEquals(Arrays.asList("pdf", "png", "jpg"), UnstructuredSourcePolicy.fileExtensions(props));
  }

  @Test
  void cloudExtensionListCannotWidenTheTypes() {
    TypedProperties props = new TypedProperties();
    props.setProperty(CloudSourceConfig.CLOUD_DATAFILE_EXTENSION.key(), "pdf,json");
    assertThrows(IllegalArgumentException.class, () -> UnstructuredSourcePolicy.fileExtensions(props));
  }

  static Stream<String> structuredExtensions() {
    return Stream.of("parquet", "orc", "avro", "json", "jsonl", "csv", "xml", "hfile");
  }

  @ParameterizedTest
  @MethodSource("structuredExtensions")
  void refusesEveryStructuredExtension(String extension) {
    TypedProperties props = new TypedProperties();
    props.setProperty(BLOB_EXTENSIONS, extension);
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> UnstructuredSourcePolicy.fileExtensions(props));
    assertTrue(failure.getMessage().contains(extension), failure.getMessage());
  }

  static Stream<Arguments> blankLists() {
    return Stream.of(BLOB_EXTENSIONS, CloudSourceConfig.CLOUD_DATAFILE_EXTENSION.key(),
        "hoodie.deltastreamer.source.cloud.data.select.file.extension")
        .flatMap(key -> Stream.of("", " ", "\t", ",", " , , ").map(value -> Arguments.of(key, value)));
  }

  @ParameterizedTest
  @MethodSource("blankLists")
  void refusesUnusableExtensionLists(String key, String value) {
    TypedProperties props = new TypedProperties();
    props.setProperty(key, value);
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> UnstructuredSourcePolicy.fileExtensions(props));
    assertTrue(failure.getMessage().contains("extensions"), failure.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {BLOB_EXTENSIONS, "hoodie.streamer.source.cloud.data.select.file.extension",
      "hoodie.deltastreamer.source.cloud.data.select.file.extension"})
  void honoursEachNarrowingListIndependently(String key) {
    TypedProperties props = new TypedProperties();
    props.setProperty(key, "png");
    assertEquals(Collections.singletonList("png"), UnstructuredSourcePolicy.fileExtensions(props));
  }

  @Test
  void legacyExtensionAliasCannotWidenTheSet() {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.deltastreamer.source.cloud.data.select.file.extension", "png,json");
    assertThrows(IllegalArgumentException.class, () -> UnstructuredSourcePolicy.fileExtensions(props));
  }

  @Test
  void disjointValidListsSelectNothing() {
    TypedProperties props = new TypedProperties();
    props.setProperty(BLOB_EXTENSIONS, "pdf");
    props.setProperty(CloudSourceConfig.CLOUD_DATAFILE_EXTENSION.key(), "png");
    assertEquals(Collections.emptyList(), UnstructuredSourcePolicy.fileExtensions(props));
  }

  @ParameterizedTest
  @ValueSource(strings = {"hoodie.streamer.source.cloud.data.datafile.format", "hoodie.deltastreamer.source.cloud.data.datafile.format"})
  void permitsExplicitEmptyFormatAndRejectsNonemptyFormatAliases(String key) {
    TypedProperties props = new TypedProperties();
    props.setProperty(key, "");
    assertEquals(13, UnstructuredSourcePolicy.fileExtensions(props).size());
    props.setProperty(key, "parquet");
    assertThrows(IllegalArgumentException.class, () -> UnstructuredSourcePolicy.fileExtensions(props));
  }
}
