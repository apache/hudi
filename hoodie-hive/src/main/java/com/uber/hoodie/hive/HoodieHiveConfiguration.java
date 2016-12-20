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

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configurations for registering a hoodie dataset into hive metastore
 */
public class HoodieHiveConfiguration {
    private final String hiveJdbcUrl;
    private final String dbName;
    private final String hiveUsername;
    private final String hivePassword;
    private final Configuration configuration;

    private HoodieHiveConfiguration(String hiveJdbcUrl, String defaultDatabaseName,
        String hiveUsername, String hivePassword, Configuration configuration) {
        this.hiveJdbcUrl = hiveJdbcUrl;
        this.dbName = defaultDatabaseName;
        this.hiveUsername = hiveUsername;
        this.hivePassword = hivePassword;
        this.configuration = configuration;
    }

    public String getHiveJdbcUrl() {
        return hiveJdbcUrl;
    }

    public String getDbName() {
        return dbName;
    }

    public String getHiveUsername() {
        return hiveUsername;
    }

    public String getHivePassword() {
        return hivePassword;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HoodieHiveConfiguration{");
        sb.append("hiveJdbcUrl='").append(hiveJdbcUrl).append('\'');
        sb.append(", dbName='").append(dbName).append('\'');
        sb.append(", hiveUsername='").append(hiveUsername).append('\'');
        sb.append(", hivePassword='").append(hivePassword).append('\'');
        sb.append(", configuration=").append(configuration);
        sb.append('}');
        return sb.toString();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private static Logger LOG = LoggerFactory.getLogger(Builder.class);
        private String hiveJdbcUrl;
        private String dbName;
        private String jdbcUsername;
        private String jdbcPassword;
        private Configuration configuration;

        public Builder hiveJdbcUrl(String hiveJdbcUrl) {
            this.hiveJdbcUrl = hiveJdbcUrl;
            return this;
        }

        public Builder hivedb(String hiveDatabase) {
            this.dbName = hiveDatabase;
            return this;
        }

        public Builder jdbcUsername(String jdbcUsername) {
            this.jdbcUsername = jdbcUsername;
            return this;
        }

        public Builder jdbcPassword(String jdbcPassword) {
            this.jdbcPassword = jdbcPassword;
            return this;
        }

        public Builder hadoopConfiguration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        public HoodieHiveConfiguration build() {
            HoodieHiveConfiguration config =
                new HoodieHiveConfiguration(hiveJdbcUrl, dbName, jdbcUsername, jdbcPassword,
                    configuration);
            LOG.info("Hoodie Hive Configuration - " + config);
            return config;
        }
    }
}
