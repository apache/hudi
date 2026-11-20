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
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;

/**
 * Failure injection for the dummy preprocessors: source_path_shas listed in {@link #FAIL_SOURCE_PATH_SHAS} fail, so tests can
 * prove per-file error isolation. Not used by real preprocessors.
 */
public final class DummyFailures {

  /** Comma list of source_path_shas the dummy preprocessors fail on; default empty. A test-only key, outside the prep prefixes (a key there must be a recognized prep setting). */
  public static final String FAIL_SOURCE_PATH_SHAS = EmbeddingTableConfig.PREFIX + "test.dummy.fail.source_path_shas";

  private DummyFailures() {
  }

  static boolean shouldFail(FileMeta file, TypedProperties props) {
    return EmbeddingTableConfig.splitList(props.getString(FAIL_SOURCE_PATH_SHAS, ""))
        .contains(file.getSourcePathSha());
  }
}
