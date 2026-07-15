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

package org.apache.hudi.common.index.vector.search;

import org.apache.hudi.common.model.HoodieRecordGlobalLocation;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * A snapshot-pinned batched Record-Level Index lookup (RFC-104 v3 §7). Given a batch of record keys,
 * returns the current live location for each key that exists at the pinned table instant; keys with
 * no entry (absent from the map) are treated as deleted.
 *
 * <p>Engine adapters supply the concrete implementation (wrapping
 * {@code readRecordIndexLocationsWithKeys} at {@code snapshot.tableInstant}). Keeping it an injected
 * SAM lets the arbiter logic stay engine-neutral and unit-testable with a fake lookup.
 */
@FunctionalInterface
public interface RecordIndexLookup extends Serializable {

  Map<String, HoodieRecordGlobalLocation> lookup(List<String> recordKeys);
}
