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

package org.apache.hudi.utilities.util;

/**
 * Which listing holds the file a finding is about.
 *
 * <p>Named rather than implied by the pattern so a consumer can filter on one field: the files
 * worth adjudicating are exactly the {@link #FS} ones, since they are the files that exist on
 * storage and are unaccounted for.
 */
public enum MismatchSide {

  /** On storage, absent from the metadata table's listing. */
  FS,

  /** Recorded by the metadata table, absent from storage. */
  MDT,

  /** Present on both sides; they disagree about the file's attributes rather than its existence. */
  BOTH
}
