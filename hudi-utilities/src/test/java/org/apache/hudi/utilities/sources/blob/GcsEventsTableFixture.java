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

package org.apache.hudi.utilities.sources.blob;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Test helper: a GCS events Hudi table in the row shape the stock GCS events source writes from Pub/Sub notifications
 * ({@code id, bucket, name, size}, the size as a string as GCS sends it); one call is one commit. The selection reads the bucket,
 * the name and the size only.
 */
public final class GcsEventsTableFixture {

  private static final ObjectMapper JSON = new ObjectMapper();

  /** One GCS object notification: an object of {@code size} bytes at {@code gs://bucket/name}. */
  public static final class Notification {
    final String bucket;
    final String name;
    final long size;

    public Notification(String bucket, String name, long size) {
      this.bucket = bucket;
      this.name = name;
      this.size = size;
    }

    String toJson() {
      ObjectNode node = JSON.createObjectNode()
          .put("id", bucket + "/" + name + "/" + UUID.randomUUID())
          .put("bucket", bucket)
          .put("name", name)
          .put("size", String.valueOf(size));
      try {
        return JSON.writeValueAsString(node);
      } catch (JsonProcessingException e) {
        throw new IllegalStateException("Cannot write the GCS notification of " + name, e);
      }
    }
  }

  private GcsEventsTableFixture() {
  }

  /** Appends {@code notifications} to the GCS events table at {@code basePath} as one commit (creating the table on first use). */
  public static void append(SparkSession spark, String basePath, List<Notification> notifications) {
    List<String> json = notifications.stream().map(Notification::toJson).collect(Collectors.toList());
    Dataset<Row> rows = spark.read().schema("id STRING, bucket STRING, name STRING, size STRING")
        .json(spark.createDataset(json, Encoders.STRING()));
    rows.write().format("hudi")
        .option("hoodie.table.name", "gcs_events")
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
        .option("hoodie.datasource.write.partitionpath.field", "")
        .option("hoodie.datasource.write.operation", "insert")
        .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
        .option("hoodie.metadata.enable", "false")
        .mode(SaveMode.Append)
        .save(basePath);
  }
}
