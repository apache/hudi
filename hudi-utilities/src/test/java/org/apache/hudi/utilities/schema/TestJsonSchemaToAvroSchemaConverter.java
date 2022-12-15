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

package org.apache.hudi.utilities.schema;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class TestJsonSchemaToAvroSchemaConverter {

  @Test
  void testConvertJsonSchemaToAvroSchema() throws IOException {
    String jsonSchema =
        "{\"type\":\"object\",\"title\":\"cdc_marqeta_jcard_tranlog.Envelope\",\"properties\":{\"prg_id\":{\"type\":\"integer\",\"connect.index\":6,\"connect.type\":\"int32\"},"
            + "\"op\":{\"type\":\"string\",\"connect.index\":3},\"entry_createdate\":{\"type\":\"string\",\"connect.index\":9},\"before\":{\"connect.index\":0,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"object\",\"title\":\"cdc_marqeta_jcard_tranlog.Value\",\"properties\":{\"date\":{\"connect.index\":25,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"title\":\"io"
            + ".debezium.time.Timestamp\",\"connect.version\":1,\"connect.type\":\"int64\"}]},\"fileName\":{\"connect.index\":15,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},"
            + "\"functionCode\":{\"connect.index\":13,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"remoteHost\":{\"type\":\"string\",\"default\":\"localhost\",\"connect.index\":69},"
            + "\"localTransactionDate\":{\"connect.index\":27,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"title\":\"io.debezium.time.Timestamp\",\"connect.version\":1,\"connect"
            + ".type\":\"int64\"}]},\"glTransaction\":{\"connect.index\":52,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},\"ca_street\":{\"connect.index\":54,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"retrievalReferenceNumber\":{\"connect.index\":60,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},"
            + "\"acquirer\":{\"connect.index\":4,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"mcc\":{\"connect.index\":53,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},"
            + "\"tid\":{\"connect.index\":7,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"responseCode\":{\"connect.index\":16,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},"
            + "\"itc\":{\"connect.index\":31,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"createdTime\":{\"type\":\"string\",\"title\":\"io.debezium.time.ZonedTimestamp\","
            + "\"default\":\"1970-01-01T00:00:00Z\",\"connect.index\":72,\"connect.version\":1},\"returnedBalances\":{\"connect.index\":39,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},"
            + "\"id\":{\"type\":\"integer\",\"connect.index\":0,\"connect.type\":\"int64\"},\"issuerFee\":{\"connect.index\":38,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"number\",\"title\":\"org"
            + ".apache.kafka.connect.data.Decimal\",\"connect.version\":1,\"connect.type\":\"bytes\",\"connect.parameters\":{\"scale\":\"4\",\"connect.decimal.precision\":\"14\"}}]},"
            + "\"committedUsed\":{\"connect.index\":61,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"number\",\"title\":\"org.apache.kafka.connect.data.Decimal\",\"connect.version\":1,\"connect"
            + ".type\":\"bytes\",\"connect.parameters\":{\"scale\":\"2\",\"connect.decimal.precision\":\"14\"}}]},\"batchNumber\":{\"connect.index\":30,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"integer\",\"connect.type\":\"int64\"}]},\"adviceId\":{\"connect.index\":71,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},"
            + "\"additionalAmount\":{\"connect.index\":36,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"number\",\"title\":\"org.apache.kafka.connect.data.Decimal\",\"connect.version\":1,\"connect"
            + ".type\":\"bytes\",\"connect.parameters\":{\"scale\":\"2\",\"connect.decimal.precision\":\"14\"}}]},\"ca_mallName\":{\"connect.index\":57,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"string\"}]},\"multiClearCount\":{\"connect.index\":75,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int16\"}]},\"settlementDate\":{\"connect"
            + ".index\":29,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"title\":\"io.debezium.time.Date\",\"connect.version\":1,\"connect.type\":\"int32\"}]},\"tags\":{\"connect"
            + ".index\":64,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"actingCardHolder\":{\"connect.index\":48,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect"
            + ".type\":\"int64\"}]},\"node\":{\"connect.index\":2,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"ca_name\":{\"connect.index\":9,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"string\"}]},\"rc\":{\"connect.index\":40,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"originalItc\":{\"connect.index\":33,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"string\"}]},\"ca_zip\":{\"connect.index\":55,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"expirationTime\":{\"connect.index\":74,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\",\"title\":\"io.debezium.time.ZonedTimestamp\",\"connect.version\":1}]},\"account2\":{\"connect.index\":51,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},\"networkReferenceId\":{\"connect.index\":58,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"string\"}]},\"incomingNetworkRequestITC\":{\"connect.index\":76,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"captureDate\":{\"connect.index\":28,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"title\":\"io.debezium.time.Date\",\"connect.version\":1,\"connect.type\":\"int32\"}]},\"digitalWalletToken\":{\"connect"
            + ".index\":78,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},\"reversalCount\":{\"connect.index\":19,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"integer\",\"connect.type\":\"int32\"}]},\"card\":{\"connect.index\":49,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},"
            + "\"request\":{\"connect.index\":44,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\",\"connect.type\":\"bytes\"}]},\"completionId\":{\"connect.index\":22,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},\"lastModifiedTime\":{\"type\":\"string\",\"title\":\"io.debezium.time.ZonedTimestamp\","
            + "\"default\":\"1970-01-01T00:00:00Z\",\"connect.index\":73,\"connect.version\":1},\"networkMid\":{\"connect.index\":66,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},"
            + "\"mid\":{\"connect.index\":6,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"originator\":{\"connect.index\":5,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},"
            + "\"requestAmount\":{\"connect.index\":67,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"number\",\"title\":\"org.apache.kafka.connect.data.Decimal\",\"connect.version\":1,\"connect"
            + ".type\":\"bytes\",\"connect.parameters\":{\"scale\":\"2\",\"connect.decimal.precision\":\"14\"}}]},\"localId\":{\"connect.index\":1,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"integer\",\"connect.type\":\"int64\"}]},\"feeRate\":{\"connect.index\":62,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"number\",\"title\":\"org.apache.kafka.connect.data"
            + ".Decimal\",\"connect.version\":1,\"connect.type\":\"bytes\",\"connect.parameters\":{\"scale\":\"3\",\"connect.decimal.precision\":\"6\"}}]},\"ca_country\":{\"connect.index\":12,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"network\":{\"connect.index\":3,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"duration\":{\"connect.index\":42,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int32\"}]},\"forwardingInstId\":{\"connect.index\":65,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},"
            + "\"displayMessage\":{\"connect.index\":18,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"ca_city\":{\"connect.index\":10,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"string\"}]},\"completionCount\":{\"connect.index\":21,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int32\"}]},\"extrc\":{\"connect.index\":41,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"responseAmount\":{\"connect.index\":70,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"number\",\"title\":\"org.apache.kafka"
            + ".connect.data.Decimal\",\"connect.version\":1,\"connect.type\":\"bytes\",\"connect.parameters\":{\"scale\":\"2\",\"connect.decimal.precision\":\"14\"}}]},\"reversalId\":{\"connect"
            + ".index\":20,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},\"reasonCode\":{\"connect.index\":14,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"string\"}]},\"irc\":{\"connect.index\":32,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"subNetwork\":{\"connect.index\":77,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"string\"}]},\"amount\":{\"connect.index\":35,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"number\",\"title\":\"org.apache.kafka.connect.data.Decimal\",\"connect"
            + ".version\":1,\"connect.type\":\"bytes\",\"connect.parameters\":{\"scale\":\"2\",\"connect.decimal.precision\":\"14\"}}]},\"outstanding\":{\"connect.index\":43,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int32\"}]},\"voidId\":{\"connect.index\":24,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect"
            + ".type\":\"int64\"}]},\"cardHolder\":{\"connect.index\":47,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},\"acquirerReferenceId\":{\"connect"
            + ".index\":59,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"approvalNumber\":{\"connect.index\":17,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},"
            + "\"acquirerFee\":{\"connect.index\":37,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"number\",\"title\":\"org.apache.kafka.connect.data.Decimal\",\"connect.version\":1,\"connect"
            + ".type\":\"bytes\",\"connect.parameters\":{\"scale\":\"4\",\"connect.decimal.precision\":\"14\"}}]},\"transmissionDate\":{\"connect.index\":26,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"integer\",\"title\":\"io.debezium.time.Timestamp\",\"connect.version\":1,\"connect.type\":\"int64\"}]},\"ca_region\":{\"connect.index\":11,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"string\"}]},\"response\":{\"connect.index\":45,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\",\"connect.type\":\"bytes\"}]},\"ca_storeNumber\":{\"connect.index\":56,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"stan\":{\"connect.index\":8,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"gatewaylog\":{\"connect.index\":63,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},\"refId\":{\"connect.index\":46,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect"
            + ".type\":\"int64\"}]},\"currencyCode\":{\"connect.index\":34,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"transactionState\":{\"connect.index\":68,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"account\":{\"connect.index\":50,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},"
            + "\"voidCount\":{\"connect.index\":23,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int32\"}]}}}]},\"shard_id\":{\"type\":\"integer\",\"connect.index\":8,"
            + "\"connect.type\":\"int32\"},\"server_transaction_id\":{\"type\":\"integer\",\"connect.index\":11,\"connect.type\":\"int64\"},\"after\":{\"connect.index\":1,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"object\",\"title\":\"cdc_marqeta_jcard_tranlog.Value\",\"properties\":{\"date\":{\"connect.index\":25,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"integer\",\"title\":\"io.debezium.time.Timestamp\",\"connect.version\":1,\"connect.type\":\"int64\"}]},\"fileName\":{\"connect.index\":15,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"string\"}]},\"functionCode\":{\"connect.index\":13,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"remoteHost\":{\"type\":\"string\",\"default\":\"localhost\","
            + "\"connect.index\":69},\"localTransactionDate\":{\"connect.index\":27,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"title\":\"io.debezium.time.Timestamp\",\"connect"
            + ".version\":1,\"connect.type\":\"int64\"}]},\"glTransaction\":{\"connect.index\":52,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},"
            + "\"ca_street\":{\"connect.index\":54,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"retrievalReferenceNumber\":{\"connect.index\":60,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"string\"}]},\"acquirer\":{\"connect.index\":4,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"mcc\":{\"connect.index\":53,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"string\"}]},\"tid\":{\"connect.index\":7,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"responseCode\":{\"connect.index\":16,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"string\"}]},\"itc\":{\"connect.index\":31,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"createdTime\":{\"type\":\"string\",\"title\":\"io.debezium.time"
            + ".ZonedTimestamp\",\"default\":\"1970-01-01T00:00:00Z\",\"connect.index\":72,\"connect.version\":1},\"returnedBalances\":{\"connect.index\":39,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"string\"}]},\"id\":{\"type\":\"integer\",\"connect.index\":0,\"connect.type\":\"int64\"},\"issuerFee\":{\"connect.index\":38,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"number\",\"title\":\"org.apache.kafka.connect.data.Decimal\",\"connect.version\":1,\"connect.type\":\"bytes\",\"connect.parameters\":{\"scale\":\"4\",\"connect.decimal"
            + ".precision\":\"14\"}}]},\"committedUsed\":{\"connect.index\":61,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"number\",\"title\":\"org.apache.kafka.connect.data.Decimal\",\"connect"
            + ".version\":1,\"connect.type\":\"bytes\",\"connect.parameters\":{\"scale\":\"2\",\"connect.decimal.precision\":\"14\"}}]},\"batchNumber\":{\"connect.index\":30,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},\"adviceId\":{\"connect.index\":71,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect"
            + ".type\":\"int64\"}]},\"additionalAmount\":{\"connect.index\":36,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"number\",\"title\":\"org.apache.kafka.connect.data.Decimal\",\"connect"
            + ".version\":1,\"connect.type\":\"bytes\",\"connect.parameters\":{\"scale\":\"2\",\"connect.decimal.precision\":\"14\"}}]},\"ca_mallName\":{\"connect.index\":57,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"multiClearCount\":{\"connect.index\":75,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int16\"}]},"
            + "\"settlementDate\":{\"connect.index\":29,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"title\":\"io.debezium.time.Date\",\"connect.version\":1,\"connect.type\":\"int32\"}]},"
            + "\"tags\":{\"connect.index\":64,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"actingCardHolder\":{\"connect.index\":48,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\","
            + "\"connect.type\":\"int64\"}]},\"node\":{\"connect.index\":2,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"ca_name\":{\"connect.index\":9,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"string\"}]},\"rc\":{\"connect.index\":40,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"originalItc\":{\"connect.index\":33,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"string\"}]},\"ca_zip\":{\"connect.index\":55,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"expirationTime\":{\"connect.index\":74,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\",\"title\":\"io.debezium.time.ZonedTimestamp\",\"connect.version\":1}]},\"account2\":{\"connect.index\":51,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},\"networkReferenceId\":{\"connect.index\":58,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"string\"}]},\"incomingNetworkRequestITC\":{\"connect.index\":76,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"captureDate\":{\"connect.index\":28,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"title\":\"io.debezium.time.Date\",\"connect.version\":1,\"connect.type\":\"int32\"}]},\"digitalWalletToken\":{\"connect"
            + ".index\":78,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},\"reversalCount\":{\"connect.index\":19,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"integer\",\"connect.type\":\"int32\"}]},\"card\":{\"connect.index\":49,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},"
            + "\"request\":{\"connect.index\":44,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\",\"connect.type\":\"bytes\"}]},\"completionId\":{\"connect.index\":22,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},\"lastModifiedTime\":{\"type\":\"string\",\"title\":\"io.debezium.time.ZonedTimestamp\","
            + "\"default\":\"1970-01-01T00:00:00Z\",\"connect.index\":73,\"connect.version\":1},\"networkMid\":{\"connect.index\":66,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},"
            + "\"mid\":{\"connect.index\":6,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"originator\":{\"connect.index\":5,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},"
            + "\"requestAmount\":{\"connect.index\":67,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"number\",\"title\":\"org.apache.kafka.connect.data.Decimal\",\"connect.version\":1,\"connect"
            + ".type\":\"bytes\",\"connect.parameters\":{\"scale\":\"2\",\"connect.decimal.precision\":\"14\"}}]},\"localId\":{\"connect.index\":1,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"integer\",\"connect.type\":\"int64\"}]},\"feeRate\":{\"connect.index\":62,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"number\",\"title\":\"org.apache.kafka.connect.data"
            + ".Decimal\",\"connect.version\":1,\"connect.type\":\"bytes\",\"connect.parameters\":{\"scale\":\"3\",\"connect.decimal.precision\":\"6\"}}]},\"ca_country\":{\"connect.index\":12,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"network\":{\"connect.index\":3,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"duration\":{\"connect.index\":42,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int32\"}]},\"forwardingInstId\":{\"connect.index\":65,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},"
            + "\"displayMessage\":{\"connect.index\":18,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"ca_city\":{\"connect.index\":10,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"string\"}]},\"completionCount\":{\"connect.index\":21,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int32\"}]},\"extrc\":{\"connect.index\":41,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"responseAmount\":{\"connect.index\":70,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"number\",\"title\":\"org.apache.kafka"
            + ".connect.data.Decimal\",\"connect.version\":1,\"connect.type\":\"bytes\",\"connect.parameters\":{\"scale\":\"2\",\"connect.decimal.precision\":\"14\"}}]},\"reversalId\":{\"connect"
            + ".index\":20,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},\"reasonCode\":{\"connect.index\":14,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"string\"}]},\"irc\":{\"connect.index\":32,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"subNetwork\":{\"connect.index\":77,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"string\"}]},\"amount\":{\"connect.index\":35,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"number\",\"title\":\"org.apache.kafka.connect.data.Decimal\",\"connect"
            + ".version\":1,\"connect.type\":\"bytes\",\"connect.parameters\":{\"scale\":\"2\",\"connect.decimal.precision\":\"14\"}}]},\"outstanding\":{\"connect.index\":43,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int32\"}]},\"voidId\":{\"connect.index\":24,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect"
            + ".type\":\"int64\"}]},\"cardHolder\":{\"connect.index\":47,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},\"acquirerReferenceId\":{\"connect"
            + ".index\":59,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"approvalNumber\":{\"connect.index\":17,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},"
            + "\"acquirerFee\":{\"connect.index\":37,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"number\",\"title\":\"org.apache.kafka.connect.data.Decimal\",\"connect.version\":1,\"connect"
            + ".type\":\"bytes\",\"connect.parameters\":{\"scale\":\"4\",\"connect.decimal.precision\":\"14\"}}]},\"transmissionDate\":{\"connect.index\":26,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"integer\",\"title\":\"io.debezium.time.Timestamp\",\"connect.version\":1,\"connect.type\":\"int64\"}]},\"ca_region\":{\"connect.index\":11,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"string\"}]},\"response\":{\"connect.index\":45,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\",\"connect.type\":\"bytes\"}]},\"ca_storeNumber\":{\"connect.index\":56,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"stan\":{\"connect.index\":8,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"gatewaylog\":{\"connect.index\":63,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},\"refId\":{\"connect.index\":46,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect"
            + ".type\":\"int64\"}]},\"currencyCode\":{\"connect.index\":34,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"transactionState\":{\"connect.index\":68,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"account\":{\"connect.index\":50,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},"
            + "\"voidCount\":{\"connect.index\":23,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int32\"}]}}}]},\"source\":{\"type\":\"object\",\"title\":\"io.debezium"
            + ".connector.mysql.Source\",\"connect.index\":2,\"properties\":{\"query\":{\"connect.index\":14,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"thread\":{\"connect.index\":13,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},\"server_id\":{\"type\":\"integer\",\"connect.index\":8,\"connect.type\":\"int64\"},"
            + "\"version\":{\"type\":\"string\",\"connect.index\":0},\"sequence\":{\"connect.index\":6,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"file\":{\"type\":\"string\",\"connect"
            + ".index\":10},\"connector\":{\"type\":\"string\",\"connect.index\":1},\"pos\":{\"type\":\"integer\",\"connect.index\":11,\"connect.type\":\"int64\"},\"name\":{\"type\":\"string\","
            + "\"connect.index\":2},\"gtid\":{\"connect.index\":9,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"row\":{\"type\":\"integer\",\"connect.index\":12,\"connect"
            + ".type\":\"int32\"},\"ts_ms\":{\"type\":\"integer\",\"connect.index\":3,\"connect.type\":\"int64\"},\"snapshot\":{\"connect.index\":4,\"oneOf\":[{\"type\":\"null\"},"
            + "{\"type\":\"string\",\"title\":\"io.debezium.data.Enum\",\"default\":\"false\",\"connect.version\":1,\"connect.parameters\":{\"allowed\":\"true,last,false,incremental\"}}]},"
            + "\"db\":{\"type\":\"string\",\"connect.index\":5},\"table\":{\"connect.index\":7,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]}}},\"program\":{\"type\":\"string\",\"connect"
            + ".index\":7},\"ts_ms\":{\"connect.index\":4,\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},\"transaction\":{\"connect.index\":5,"
            + "\"oneOf\":[{\"type\":\"null\"},{\"type\":\"object\",\"properties\":{\"data_collection_order\":{\"type\":\"integer\",\"connect.index\":2,\"connect.type\":\"int64\"},"
            + "\"id\":{\"type\":\"string\",\"connect.index\":0},\"total_order\":{\"type\":\"integer\",\"connect.index\":1,\"connect.type\":\"int64\"}}}]},\"server_uuid\":{\"type\":\"string\","
            + "\"connect.index\":10}}}";
    String avroSchema = JsonSchemaToAvroSchemaConverter.convertJsonSchemaToAvroSchema(jsonSchema);
    Schema parse = new Schema.Parser().parse(avroSchema);

    assert !parse.isError();
    assert parse.getName().equals("tranlog");

  }
}
