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

package org.apache.hudi.common.util.injection;

public enum ErrorInjectionCategory {
  // Internal
  SKIP_ERROR_INJECTION_AT_BEGINNING("000-SKIP-INJ-BEGINNING", 0.1),
  // Core transaction in data table
  DT_WRITE_BEFORE_COMMIT("001-DT-WRITE-BEFORE-COMMIT", 0.2),
  DT_WRITE_BEFORE_MDT("002-DT-WRITE-BEFORE-MDT", 0.2),
  DT_AFTER_MDT("003-DT-AFTER-MDT", 0.2),
  DT_ROLLBACK_BEFORE_MDT("011-DT-ROLLBACK-BEFORE-MDT", 0.2),
  DT_ROLLBACK_AFTER_MDT("012-DT-ROLLBACK-AFTER-MDT", 0.2),
  // Core transaction in metadata table
  MDT_WRITE_BEFORE_COMMIT("101-MDT-WRITE-BEFORE-COMMIT", 0.2),
  MDT_PRE_COMMIT("102-MDT-PRE-COMMIT", 0.2),
  MDT_BEFORE_ARCHIVAL("111-MDT-BEFORE-ARCHIVAL", 0.1),
  MDT_AFTER_COMPACTION("112-MDT-AFTER-COMPACTION", 0.3),
  MDT_BEFORE_CLEANING("113-MDT-BEFORE-CLEANING", 0.2),
  MDT_ROLLBACK("105-MDT-ROLLBACK", 0.2),
  // Table services
  DT_COMPACTION_BEFORE_MDT("201-DT-COMPACTION-BEFORE-MDT", 0.3),
  DT_COMPACTION_AFTER_MDT("202-DT-COMPACTION-AFTER-MDT", 0.3),
  MDT_COMPACTION("203-MDT-COMPACTION", 0.3),
  DT_CLUSTERING_BEFORE_MDT("211-DT-CLUSTERING-BEFORE-MDT", 0.3),
  DT_CLUSTERING_AFTER_MDT("212-DT-CLUSTERING-AFTER-MDT", 0.3),
  MDT_CLUSTERING("213-MDT-CLUSTERING", 0.3),
  DT_CLEANING_BEFORE_MDT("221-DT-CLEANING-BEFORE-MDT", 0.3),
  DT_CLEANING_AFTER_MDT("222-DT-CLEANING-AFTER-MDT", 0.3),
  MDT_CLEANING("223-MDT-CLEANING", 0.3);

  private static final String LABEL_PREFIX = "ERROR-INJ-";

  private final String label;
  private final double probability;

  ErrorInjectionCategory(final String label, final double probability) {
    this.label = LABEL_PREFIX + label;
    this.probability = probability;
  }

  public String getLabel() {
    return label;
  }

  public double getProbability() {
    return probability;
  }

  public String getSignalFilename() {
    return "/tmp/" + label + ".txt";
  }
}
