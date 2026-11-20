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

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Test helper: an S3 events Hudi table in the row shape S3EventsSource writes from S3 event notifications
 * ({@code eventName, eventTime, s3.bucket.name, s3.object.key, s3.object.size}, and {@code s3.object.sequencer} when an
 * event of the commit carries one); one call is one commit. Nothing of the blob job reads a notification's ETag, so none is written. The key is stored
 * as given, i.e. decoded: S3EventsSource url-decodes each notification before it writes the table, so the table holds the
 * object's key itself, never the notification's encoded form.
 */
public final class S3EventsTableFixture {

  private static final ObjectMapper JSON = new ObjectMapper();

  /** One S3 event notification record. */
  public static final class Event {
    final String eventName;
    final String bucket;
    final String key;
    final Long size;
    final String eventTime;
    final String sequencer;

    Event(String eventName, String bucket, String key, Long size, String eventTime) {
      this(eventName, bucket, key, size, eventTime, null);
    }

    Event(String eventName, String bucket, String key, Long size, String eventTime, String sequencer) {
      this.eventName = eventName;
      this.bucket = bucket;
      this.key = key;
      this.size = size;
      this.eventTime = eventTime;
      this.sequencer = sequencer;
    }

    /** This event with the S3 per-object {@code sequencer} (hex, compared as S3 compares it). */
    public Event withSequencer(String sequencer) {
      return new Event(eventName, bucket, key, size, eventTime, sequencer);
    }

    /** This event with no event name (a null {@code eventName}). */
    public Event withoutName() {
      return new Event(null, bucket, key, size, eventTime, sequencer);
    }

    /** This event at {@code eventTime}. */
    public Event atTime(Instant eventTime) {
      return new Event(eventName, bucket, key, size, eventTime.toString(), sequencer);
    }

    /** The record as JSON; Jackson escapes every value, so a key may hold any character (a quote or a backslash too). */
    String toJson() {
      ObjectNode event = JSON.createObjectNode()
          .put("eventId", UUID.randomUUID().toString())
          .put("eventName", eventName)
          .put("eventTime", eventTime);
      ObjectNode s3 = event.putObject("s3");
      s3.putObject("bucket").put("name", bucket);
      ObjectNode object = s3.putObject("object").put("key", key);
      if (size != null) {
        object.put("size", size);
      }
      if (sequencer != null) {
        object.put("sequencer", sequencer);
      }
      try {
        return JSON.writeValueAsString(event);
      } catch (JsonProcessingException e) {
        throw new IllegalStateException("Cannot write the S3 event of " + key, e);
      }
    }
  }

  private S3EventsTableFixture() {
  }

  /** An ObjectCreated:Put event of the object {@code key}. */
  public static Event created(String bucket, String key, long size, Instant eventTime) {
    return new Event("ObjectCreated:Put", bucket, key, size, eventTime.toString());
  }

  /** An event of any name that carries an object size (e.g. a lifecycle expiration or a restore). */
  public static Event sized(String eventName, String bucket, String key, long size, Instant eventTime) {
    return new Event(eventName, bucket, key, size, eventTime.toString());
  }

  /** An ObjectCreated:Put event whose eventTime is the given text verbatim (e.g. a malformed one). */
  public static Event createdWithRawTime(String bucket, String key, long size, String rawEventTime) {
    return new Event("ObjectCreated:Put", bucket, key, size, rawEventTime);
  }

  /** An ObjectRemoved:Delete event, which carries no object size. */
  public static Event removed(String bucket, String key, Instant eventTime) {
    return new Event("ObjectRemoved:Delete", bucket, key, null, eventTime.toString());
  }

  /** Appends {@code events} to the events table at {@code basePath} as one commit (creating the table on first use). */
  public static void appendEvents(SparkSession spark, String basePath, List<Event> events) {
    appendEvents(spark, basePath, events, Collections.emptyMap());
  }

  /** As {@link #appendEvents(SparkSession, String, List)}, with extra Hudi write options (e.g. the metadata table off). */
  public static void appendEvents(SparkSession spark, String basePath, List<Event> events, Map<String, String> writeOptions) {
    appendEvents(spark, basePath, events, writeOptions, true);
  }

  /** As {@link #appendEvents(SparkSession, String, List)}, for an events table that has no {@code eventName} column. */
  public static void appendEventsWithoutEventNameColumn(SparkSession spark, String basePath, List<Event> events) {
    appendEvents(spark, basePath, events, Collections.emptyMap(), false);
  }

  private static void appendEvents(SparkSession spark, String basePath, List<Event> events, Map<String, String> writeOptions,
                                   boolean withEventName) {
    List<String> json = events.stream().map(Event::toJson).collect(Collectors.toList());
    boolean withSequencer = events.stream().anyMatch(event -> event.sequencer != null);
    // an explicit schema so a batch of only removals still has s3.object.size
    Dataset<Row> rows = spark.read()
        .schema("eventId STRING, " + (withEventName ? "eventName STRING, " : "") + "eventTime STRING, "
            + "s3 STRUCT<bucket: STRUCT<name: STRING>, object: STRUCT<key: STRING, size: BIGINT"
            + (withSequencer ? ", sequencer: STRING" : "") + ">>")
        .json(spark.createDataset(json, Encoders.STRING()));
    rows.write().format("hudi")
        .option("hoodie.table.name", "s3_events")
        .option("hoodie.datasource.write.recordkey.field", "eventId")
        .option("hoodie.datasource.write.precombine.field", "eventTime")
        .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
        .option("hoodie.datasource.write.partitionpath.field", "")
        .option("hoodie.datasource.write.operation", "insert")
        .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
        .options(writeOptions)
        .mode(SaveMode.Append)
        .save(basePath);
  }
}
