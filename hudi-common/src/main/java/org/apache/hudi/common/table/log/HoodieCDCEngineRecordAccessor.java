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

package org.apache.hudi.common.table.log;

/**
 * Accessor for CDC operation, record key, and before/after images in an engine-specific row.
 *
 * @param <T> Engine-specific record type used by native CDC log files
 */
public interface HoodieCDCEngineRecordAccessor<T> {

  String getOperation(T record);

  String getRecordKey(T record);

  T getImage(T record, int ordinal, int imageArity);
}
