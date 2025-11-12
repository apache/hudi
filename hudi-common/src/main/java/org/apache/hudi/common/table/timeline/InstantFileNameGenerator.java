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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;

import java.io.Serializable;

/**
 * Factory for generating instants filenames.
 */
public interface InstantFileNameGenerator extends Serializable {

  TimelineLayoutVersion getLayoutVersion();

  String makeCommitFileName(String instantTime);

  String makeInflightCommitFileName(String instantTime);

  String makeRequestedCommitFileName(String instantTime);

  String makeCleanerFileName(String instant);

  String makeRequestedCleanerFileName(String instant);

  String makeInflightCleanerFileName(String instant);

  String makeRollbackFileName(String instant);

  String makeRequestedRollbackFileName(String instant);

  String makeRequestedRestoreFileName(String instant);

  String makeInflightRollbackFileName(String instant);

  String makeInflightSavePointFileName(String instantTime);

  String makeSavePointFileName(String instantTime);

  String makeInflightDeltaFileName(String instantTime);

  String makeRequestedDeltaFileName(String instantTime);

  String makeInflightCompactionFileName(String instantTime);

  String makeRequestedCompactionFileName(String instantTime);

  String makeInflightLogCompactionFileName(String instantTime);

  String makeRequestedLogCompactionFileName(String instantTime);

  String makeRestoreFileName(String instant);

  String makeInflightRestoreFileName(String instant);

  String makeReplaceFileName(String instant);

  String makeInflightReplaceFileName(String instant);

  String makeRequestedReplaceFileName(String instant);

  String makeRequestedClusteringFileName(String instant);

  String makeInflightClusteringFileName(String instant);

  String makeDeltaFileName(String instantTime);

  String getCommitFromCommitFile(String commitFileName);

  String makeFileNameAsComplete(String fileName);

  String makeFileNameAsInflight(String fileName);

  String makeIndexCommitFileName(String instant);

  String makeInflightIndexFileName(String instant);

  String makeRequestedIndexFileName(String instant);

  String makeSchemaFileName(String instantTime);

  String makeInflightSchemaFileName(String instantTime);

  String makeRequestSchemaFileName(String instantTime);

  String getFileName(HoodieInstant instant);

  String getFileName(String completionTime, HoodieInstant instant);
}
