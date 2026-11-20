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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieErrorTableConfig;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TEST-ONLY error table: each committed round's error events are one JSON-lines file
 * {@code <hoodie.errortable.base.path>/<data instant>.jsonl}, a line {@code {"reason": ..., "payload": ...}} per event (the
 * payload parsed when it is JSON). The base path is a local directory (a plain path or a {@code file:} URI). Supports the unified and the legacy commit paths.
 */
public class JsonLinesErrorTableWriter extends BaseErrorTableWriter<ErrorEvent<Object>> {

  private final transient HoodieSparkEngineContext context;
  private final String basePath;
  private transient JavaRDD<ErrorEvent<Object>> events;
  private String pendingInstant;

  public JsonLinesErrorTableWriter(HoodieStreamer.Config cfg, SparkSession sparkSession, TypedProperties props,
                                   HoodieSparkEngineContext context, FileSystem fs) {
    super(cfg, sparkSession, props, context, fs);
    this.context = context;
    this.basePath = props.getString(HoodieErrorTableConfig.ERROR_TABLE_BASE_PATH.key());
  }

  @Override
  public void addErrorEvents(JavaRDD<ErrorEvent<Object>> errorEvents) {
    events = events == null ? errorEvents : events.union(errorEvents);
  }

  @Override
  public Option<JavaRDD<HoodieAvroIndexedRecord>> getErrorEvents(String baseTableInstantTime, Option<String> committedInstantTime) {
    throw new UnsupportedOperationException("not used by the streamer");
  }

  @Override
  public boolean upsertAndCommit(String baseTableInstantTime, Option<String> committedInstantTime) {
    return commit(upsert(baseTableInstantTime, committedInstantTime));
  }

  @Override
  public JavaRDD<WriteStatus> upsert(String baseTableInstantTime, Option<String> committedInstantTime) {
    pendingInstant = baseTableInstantTime;
    return context.getJavaSparkContext().emptyRDD();
  }

  @Override
  public boolean commit(JavaRDD<WriteStatus> writeStatuses) {
    List<String> lines = events == null ? Collections.emptyList() : events.map(JsonLinesErrorTableWriter::line).collect();
    events = null;
    if (lines.isEmpty()) {
      return true;
    }
    try {
      Path dir = localPath(basePath);
      Files.createDirectories(dir);
      Files.write(dir.resolve(pendingInstant + ".jsonl"), lines, StandardCharsets.UTF_8);
      return true;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String line(ErrorEvent<Object> event) throws IOException {
    Map<String, Object> line = new LinkedHashMap<>();
    line.put("reason", event.getReason().name());
    String payload = String.valueOf(event.getPayload());
    Object parsed;
    try {
      parsed = JsonUtils.getObjectMapper().readValue(payload, new TypeReference<Map<String, Object>>() { });
    } catch (IOException notJson) {
      parsed = payload;
    }
    line.put("payload", parsed);
    return JsonUtils.getObjectMapper().writeValueAsString(line);
  }

  /** The local directory of {@code basePath}, a plain path or a {@code file:} URI. */
  private static Path localPath(String basePath) {
    return basePath.startsWith("file:") ? Paths.get(java.net.URI.create(basePath)) : Paths.get(basePath);
  }

  /**
   * The number of error events committed for data instant {@code instant} under {@code basePath}, counted line by line without
   * parsing; 0 when that round committed none (no file is written for it).
   */
  public static long count(String basePath, String instant) throws IOException {
    Path file = localPath(basePath).resolve(instant + ".jsonl");
    if (!Files.exists(file)) {
      return 0;
    }
    long events = 0;
    try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
      while (reader.readLine() != null) {
        events++;
      }
    }
    return events;
  }

  /** Every committed error event under {@code basePath}, in instant order; empty when there is none. */
  public static List<Map<String, Object>> read(String basePath) throws IOException {
    Path dir = localPath(basePath);
    if (!Files.isDirectory(dir)) {
      return Collections.emptyList();
    }
    List<Map<String, Object>> events = new ArrayList<>();
    try (Stream<Path> files = Files.list(dir)) {
      for (Path file : files.filter(f -> f.getFileName().toString().endsWith(".jsonl")).sorted().collect(Collectors.toList())) {
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
          events.add(JsonUtils.getObjectMapper().readValue(line, new TypeReference<Map<String, Object>>() { }));
        }
      }
    }
    return events;
  }
}
