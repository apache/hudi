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

package org.apache.hudi.api.builder;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SourceSinkBuilderTestBase {

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    private static final List<Column> CREATE_COLUMNS = Arrays.asList(
            Column.physical("id", DataTypes.VARCHAR(20)),
            Column.physical("name", DataTypes.VARCHAR(20)),
            Column.physical("order_time", DataTypes.TIMESTAMP(3)),
            Column.physical("dt", DataTypes.VARCHAR(20)),
            Column.physical("hr", DataTypes.VARCHAR(20)));
    private static final UniqueConstraint CONSTRAINTS = UniqueConstraint.primaryKey("id", Arrays.asList("id"));
    public static final ResolvedSchema CREATE_TABLE_SCHEMA =
            new ResolvedSchema(
                    CREATE_COLUMNS,
                    Collections.emptyList(),
                    CONSTRAINTS);

    @BeforeEach
    void beforeEach() {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env, settings);
        tEnv.getConfig().getConfiguration().setString("execution.checkpointing.interval", "1min");
    }

    public StreamExecutionEnvironment getEnv() {
        return env;
    }

    public StreamTableEnvironment gettEnv() {
        return tEnv;
    }

    public void execute() throws Exception {
        env.execute();
    }

    public DataStream<Row> dataStreamGen() {

        tEnv.sqlUpdate("CREATE TABLE dgTable (\n" +
                "   `user_id` BIGINT,\n" +
                "   `name` STRING,\n" +
                "   `order_time` TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'rows-per-second'='100',\n" +
                "    'fields.user_id.kind'='random',\n" +
                "    'fields.user_id.min'='1',\n" +
                "    'fields.user_id.max'='10',\n" +
                "    'fields.name.length'='8'\n" +
                ")");
        DataStream<Row> dataDataStream = tEnv.toAppendStream(tEnv.sqlQuery("select *, '2022-04-21' as dt, '12' as hr from dgTable"), Row.class);
        return dataDataStream;
    }
}
