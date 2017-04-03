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

package com.uber.hoodie.hive.example;

import com.uber.hoodie.hive.HiveSyncTool;
import com.uber.hoodie.hive.HiveSyncConfig;

/**
 * Example showing how to sync the dataset, written by `HoodieClientExample`
 */
public class HoodieHiveSyncExample {

    public static void main(String[] args) {

        HiveSyncConfig cfg = new HiveSyncConfig();
        cfg.databaseName = "default";
        cfg.tableName = "uber_trips";
        cfg.basePath = "/tmp/hoodie/sample-table/";
        cfg.hiveUser = "hive";
        cfg.hivePass = "hive";
        cfg.jdbcUrl = "jdbc:hive2://localhost:10010/";

        HiveSyncTool.sync(cfg);
    }
}
