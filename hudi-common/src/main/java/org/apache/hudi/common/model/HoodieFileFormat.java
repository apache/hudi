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

package org.apache.hudi.common.model;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Hoodie file format.
 */
public enum HoodieFileFormat {
  PARQUET(".parquet"),
  HOODIE_LOG(".log"),
  HFILE(".hfile"),
  ORC(".orc");

  public static final Set<String> BASE_FILE_EXTENSIONS = Arrays.stream(HoodieFileFormat.values())
      .map(HoodieFileFormat::getFileExtension)
      .filter(x -> !x.equals(HoodieFileFormat.HOODIE_LOG.getFileExtension()))
      .collect(Collectors.toCollection(HashSet::new));

  private final String extension;

  HoodieFileFormat(String extension) {
    this.extension = extension;
  }

  public String getFileExtension() {
    return extension;
  }
}
