/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.hive;

import com.uber.hoodie.hive.client.HoodieFSClient;
import com.uber.hoodie.hive.model.HoodieDatasetReference;
import parquet.schema.MessageType;

/**
 * Abstraction to get the Parquet schema for a {@link HoodieDatasetReference}
 * If you are managing the schemas externally, connect to the system and get the schema.
 *
 * @see PartitionStrategy
 */
public interface SchemaStrategy {
    MessageType getDatasetSchema(HoodieDatasetReference metadata, HoodieFSClient fsClient);
}
