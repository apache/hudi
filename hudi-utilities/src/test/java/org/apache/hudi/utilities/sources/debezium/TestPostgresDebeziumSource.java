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

package org.apache.hudi.utilities.sources.debezium;

import org.apache.hudi.common.model.debezium.DebeziumConstants;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPostgresDebeziumSource extends TestAbstractDebeziumSource {

  private static final String POSTGRES_GITHUB_SCHEMA = "{\"connect.name\": \"postgres.ghschema.gharchive.Envelope\",\n"
      + "  \"fields\": [{\"default\": null,\"name\": \"before\",\"type\": [\"null\",{\"connect.name\": \"postgres.ghschema.gharchive.Value\",\n"
      + "  \"fields\": [{\"name\": \"id\",\"type\": \"string\"},{\"name\": \"date\",\"type\": \"string\"},{\"default\": null,\"name\": \"timestamp\",\n"
      + "  \"type\": [\"null\",\"long\"]},{\"default\": null,\"name\": \"type\",\"type\": [\"null\",\"string\"]},{\"default\": null,\"name\": \"payload\",\n"
      + "  \"type\": [\"null\",\"string\"]},{\"default\": null,\"name\": \"org\",\"type\": [\"null\",\"string\"]},{\"default\": null,\"name\": \"created_at\",\n"
      + "  \"type\": [\"null\",\"long\"]},{\"default\": null,\"name\": \"public\",\"type\": [\"null\",\"boolean\"]}],\"name\": \"Value\",\"type\": \"record\"\n"
      + "  }]},{\"default\": null,\"name\": \"after\",\"type\": [\"null\",\"Value\"]},{\"name\": \"source\",\"type\": {\"connect.name\": \"io.debezium.connector.postgresql.Source\",\n"
      + "  \"fields\": [{\"name\": \"connector\",\"type\": \"string\"},{\"name\": \"name\",\"type\": \"string\"},{\"name\": \"ts_ms\",\"type\": \"long\"},\n"
      + "  {\"name\": \"db\",\"type\": \"string\"},{\"name\": \"schema\",\"type\": \"string\"},{\"name\": \"table\",\"type\": \"string\"},{\"default\": null,\n"
      + "  \"name\": \"txId\",\"type\": [\"null\",\"long\"]},{\"default\": null,\"name\": \"lsn\",\"type\": [\"null\",\"long\"]},{\"default\": null,\n"
      + "  \"name\": \"xmin\",\"type\": [\"null\",\"long\"]}],\"name\": \"Source\",\"namespace\": \"io.debezium.connector.postgresql\",\"type\": \"record\"\n"
      + "  }},{\"name\": \"op\",\"type\": \"string\"},{\"default\": null,\"name\": \"ts_ms\",\"type\": [\"null\",\"long\"]},{\"default\": null,\"name\": \"transaction\",\n"
      + "  \"type\": [\"null\",{\"fields\": [{\"name\": \"id\",\"type\": \"string\"},{\"name\": \"total_order\",\"type\": \"long\"},{\"name\": \"data_collection_order\",\n"
      + "  \"type\": \"long\"}],\"name\": \"ConnectDefault\",\"namespace\": \"io.confluent.connect.avro\",\"type\": \"record\"}]}],\"name\": \"Envelope\",\n"
      + "  \"namespace\": \"postgres.ghschema.gharchive\",\"type\": \"record\"}";

  private static final String TEST_DB = "postgres";
  private static final String TEST_SCHEMA = "ghschema";
  private static final String TEST_TABLE = "gharchive";
  private static final long TEST_TS_MS = 12345L;
  private static final long TEST_TXID = 543L;
  private static final long TEST_LSN = 98765L;

  @Override
  protected String getIndexName() {
    return "postgres";
  }

  @Override
  protected String getSourceClass() {
    return PostgresDebeziumSource.class.getName();
  }

  @Override
  protected String getSchema() {
    return POSTGRES_GITHUB_SCHEMA;
  }

  @Override
  protected GenericRecord generateMetaFields(GenericRecord rec) {
    Schema schema = new Schema.Parser().parse(getSchema());
    // Source fields specific to Postgres DB
    GenericRecord sourceRecord = new GenericData.Record(schema.getField(DebeziumConstants.INCOMING_SOURCE_FIELD).schema());
    sourceRecord.put("name", getIndexName());
    sourceRecord.put("connector", getIndexName());
    sourceRecord.put("db", TEST_DB);
    sourceRecord.put("schema", TEST_SCHEMA);
    sourceRecord.put("table", TEST_TABLE);
    sourceRecord.put("ts_ms", TEST_TS_MS);
    sourceRecord.put("txId", TEST_TXID);
    sourceRecord.put("lsn", TEST_LSN);
    rec.put(DebeziumConstants.INCOMING_SOURCE_FIELD, sourceRecord);
    return rec;
  }

  @Override
  protected void validateMetaFields(Dataset<Row> records) {
    assertTrue(records.select(DebeziumConstants.FLATTENED_TS_COL_NAME).collectAsList().stream()
        .allMatch(r -> r.getLong(0) == TEST_TS_MS));
    assertTrue(records.select(DebeziumConstants.FLATTENED_TX_ID_COL_NAME).collectAsList().stream()
        .allMatch(r -> r.getLong(0) == TEST_TXID));
    assertTrue(records.select(DebeziumConstants.FLATTENED_LSN_COL_NAME).collectAsList().stream()
        .allMatch(r -> r.getLong(0) == TEST_LSN));
  }
}
