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

package com.uber.hoodie.hive.model;


import java.util.Objects;

/**
 * A reference to a Dataset. Each dataset will have a hadoop configuration, table name,
 * base path in HDFS. {@link HoodieDatasetReference} is immutable.
 */
public class HoodieDatasetReference {
    private String tableName;
    private String baseDatasetPath;
    private String databaseName;

    public HoodieDatasetReference(String tableName, String baseDatasetPath, String databaseName) {
        this.tableName = tableName;
        this.baseDatasetPath = baseDatasetPath;
        this.databaseName = databaseName;
    }

    public String getDatabaseTableName() {
        return databaseName + "." + tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getBaseDatasetPath() {
        return baseDatasetPath;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        HoodieDatasetReference that = (HoodieDatasetReference) o;
        return Objects.equals(tableName, that.tableName) &&
            Objects.equals(baseDatasetPath, that.baseDatasetPath) &&
            Objects.equals(databaseName, that.databaseName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, baseDatasetPath, databaseName);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HoodieDatasetReference{");
        sb.append("tableName='").append(tableName).append('\'');
        sb.append(", baseDatasetPath='").append(baseDatasetPath).append('\'');
        sb.append(", databaseName='").append(databaseName).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
