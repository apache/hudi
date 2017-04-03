/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.hive;

import com.beust.jcommander.Parameter;

import java.io.Serializable;

/**
 * Configs needed to sync data into Hive.
 */
public class HiveSyncConfig implements Serializable {

    @Parameter(names = {"--database"}, description = "name of the target database in Hive", required = true)
    public String databaseName;

    @Parameter(names = {"--table"}, description = "name of the target table in Hive", required = true)
    public String tableName;

    @Parameter(names = {"--user"}, description = "Hive username", required = true)
    public String hiveUser;

    @Parameter(names = {"--pass"}, description = "Hive password", required = true)
    public String hivePass;

    @Parameter(names = {"--jdbc-url"}, description = "Hive jdbc connect url", required = true)
    public String jdbcUrl;

    @Parameter(names = {"--base-path"}, description = "Basepath of hoodie dataset to sync", required = true)
    public String basePath;

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
}
