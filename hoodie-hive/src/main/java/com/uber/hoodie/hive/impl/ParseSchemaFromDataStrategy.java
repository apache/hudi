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

package com.uber.hoodie.hive.impl;

import com.uber.hoodie.hive.HoodieHiveDatasetException;
import com.uber.hoodie.hive.SchemaStrategy;
import com.uber.hoodie.hive.client.HoodieFSClient;
import com.uber.hoodie.hive.model.HoodieDatasetReference;
import org.apache.hadoop.fs.Path;
import parquet.schema.MessageType;

import java.io.IOException;

/**
 * Schema strategy to read the parquet schema from any of the data file
 */
public class ParseSchemaFromDataStrategy implements SchemaStrategy {
    @Override
    public MessageType getDatasetSchema(HoodieDatasetReference metadata, HoodieFSClient fsClient) {
        Path anyDataFile = fsClient.lastDataFileForDataset(metadata, metadata.getBaseDatasetPath());
        try {
            return fsClient.readSchemaFromDataFile(anyDataFile);
        } catch (IOException e) {
            throw new HoodieHiveDatasetException(
                "Could not read schema for " + metadata + ", tried to read schema from "
                    + anyDataFile, e);
        }
    }
}
