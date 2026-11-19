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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.config.CloudSourceConfig;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestColumnarFileMaterializer {

  private static final long MB = 1024L * 1024L;

  private static TypedProperties propsWith(String extension) {
    TypedProperties props = new TypedProperties();
    if (extension != null) {
      props.setProperty(CloudSourceConfig.CLOUD_DATAFILE_EXTENSION.key(), extension);
    }
    return props;
  }

  private static List<CloudObjectMetadata> objectsOf(long... sizes) {
    CloudObjectMetadata[] objects = new CloudObjectMetadata[sizes.length];
    for (int i = 0; i < sizes.length; i++) {
      objects[i] = new CloudObjectMetadata("s3://b/o" + i, sizes[i]);
    }
    return Arrays.asList(objects);
  }

  @Test
  void partitionCountInflatesForHoodieMetaFields() {
    TypedProperties props = propsWith(null);
    // 100MB inflates to 110MB, which needs a third 52MB partition where the raw size needs two
    assertEquals(3, new ColumnarFileMaterializer(props)
        .partitionCount(objectsOf(100 * MB), 52 * MB, 1));
  }

  @Test
  void partitionCountNeverDropsBelowOne() {
    assertEquals(1, new ColumnarFileMaterializer(propsWith(null))
        .partitionCount(Collections.emptyList(), 10 * MB, 1));
  }

  @Test
  void partitionCountRespectsTheMinimum() {
    assertEquals(8, new ColumnarFileMaterializer(propsWith(null))
        .partitionCount(objectsOf(MB), 10 * MB, 8));
  }

  @Test
  void objectKeyPredicateUsesTheConfiguredExtension() {
    assertEquals(" and key like '%json'",
        new ColumnarFileMaterializer(propsWith("json")).objectKeyPredicate("key", propsWith("json")));
  }

  @Test
  void objectKeyPredicateMatchesAnyOfSeveralExtensions() {
    TypedProperties props = propsWith("pdf,docx");
    assertEquals(" and (key like '%pdf' or key like '%docx')",
        new ColumnarFileMaterializer(props).objectKeyPredicate("key", props));
  }

  @Test
  void objectKeyPredicateFallsBackWhenTheConfiguredValueHoldsNoExtension() {
    // an empty predicate would select every object and read it as the file format, so a value
    // that parses to nothing must fall through to the format rather than drop the predicate
    TypedProperties props = propsWith(",");
    assertEquals(" and key like '%parquet'",
        new ColumnarFileMaterializer(props).objectKeyPredicate("key", props));
  }
}
