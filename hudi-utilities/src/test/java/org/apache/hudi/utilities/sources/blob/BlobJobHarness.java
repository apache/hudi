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

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.utilities.config.HoodieIncrSourceConfig;
import org.apache.hudi.utilities.config.S3EventsHoodieIncrSourceConfig;
import org.apache.hudi.utilities.sources.UnstructuredDataSource;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.streamer.JsonLinesErrorTableWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * TEST-ONLY harness of the blob job: objects in buckets on the local disk, read through {@link BucketFolderFileSystem}
 * ({@code hoodie.streamer.source.s3incr.fs.prefix=bucketfs}), announced by an S3 events Hudi table
 * ({@link S3EventsTableFixture}), and HoodieStreamer with {@link UnstructuredDataSource} (mode blob) writing the blob table. This
 * gives tests their input objects on the local disk; tests whose point is S3 itself use LocalStack.
 */
public final class BlobJobHarness {

  static final String PROPS_RESOURCE = "/streamer-config/blob-table/blob-table.properties";
  /** The one bucket the harness's objects live in unless a test names another. */
  public static final String BUCKET = "landing";
  /** Event time of the harness's announcements; nothing in the blob table depends on it. */
  public static final Instant EVENT_TIME = Instant.parse("2026-01-02T10:00:00Z");

  private final Path objectsRoot;
  private final String eventsPath;

  /**
   * @param objectsRoot local folder holding one folder per bucket
   * @param eventsPath  base path of the S3 events table
   */
  public BlobJobHarness(Path objectsRoot, String eventsPath) {
    this.objectsRoot = objectsRoot;
    this.eventsPath = eventsPath;
  }

  /** A harness under {@code root}: objects in {@code root/objects}, the events table at {@code root/events}. */
  public static BlobJobHarness under(Path root) {
    return new BlobJobHarness(root.resolve("objects"), root.resolve("events").toUri().toString());
  }

  /** Points {@code conf} (the Spark context's Hadoop configuration) at this harness's buckets. */
  public BlobJobHarness install(Configuration conf) {
    BucketFolderFileSystem.install(conf, objectsRoot);
    return this;
  }

  public Path objectsRoot() {
    return objectsRoot;
  }

  public String eventsPath() {
    return eventsPath;
  }

  /** The local file of an object of {@link #BUCKET}. */
  public Path file(String key) {
    return BucketFolderFileSystem.file(objectsRoot, BUCKET, key);
  }

  /**
   * The blob table's record key of one version of an object, as the key generators build it from the three key columns:
   * {@code source_path_sha:<sha>,source_modification_time:<ms>,source_file_size:<bytes>}.
   */
  public static String versionKey(String sourcePath, long modificationTime, long size) {
    return BlobTableSchema.SOURCE_PATH_SHA + ":" + BlobFileNames.sourcePathSha(sourcePath) + "," + BlobTableSchema.SOURCE_MODIFICATION_TIME
        + ":" + modificationTime + "," + BlobTableSchema.SOURCE_FILE_SIZE + ":" + size;
  }

  /** The record key of the version of the object {@code key} of {@link #BUCKET} as its file is now (modification time and length). */
  public String versionKey(String key) throws IOException {
    return versionKey(sourcePath(key), Files.getLastModifiedTime(file(key)).toMillis(), Files.size(file(key)));
  }

  /** Sets the modification time of the object {@code key} of {@link #BUCKET}, so a test controls its version tag. */
  public void touch(String key, Instant modificationTime) throws IOException {
    Files.setLastModifiedTime(file(key), FileTime.from(modificationTime));
  }

  /** The object's URL as the blob source builds it, and its canonical form ({@code source_file_path}). */
  public static String sourcePath(String bucket, String key) {
    return BucketFolderFileSystem.SCHEME + "://" + bucket + "/" + key;
  }

  /** {@link #sourcePath(String, String)} of an object of {@link #BUCKET}. */
  public static String sourcePath(String key) {
    return sourcePath(BUCKET, key);
  }

  /** Writes an object of {@link #BUCKET} and returns its ObjectCreated event. */
  public S3EventsTableFixture.Event put(String key, byte[] bytes) throws IOException {
    return put(BUCKET, key, bytes);
  }

  /** Writes an object and returns its ObjectCreated event. The file's modification time is the file system's. */
  public S3EventsTableFixture.Event put(String bucket, String key, byte[] bytes) throws IOException {
    Path file = BucketFolderFileSystem.file(objectsRoot, bucket, key);
    Files.createDirectories(file.getParent());
    Files.write(file, bytes);
    return S3EventsTableFixture.created(bucket, key, bytes.length, EVENT_TIME);
  }

  /**
   * Sets a JSON-lines error table at {@code errorTablePath} into {@code props} ({@code JsonLinesErrorTableWriter}, test only):
   * with it, a blob record whose object is gone is quarantined there and the round commits. Returns {@code props}.
   */
  public static TypedProperties withErrorTable(TypedProperties props, String errorTablePath) {
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), "true");
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_BASE_PATH.key(), errorTablePath);
    props.setProperty(HoodieErrorTableConfig.ERROR_TARGET_TABLE.key(), "blob_errors");
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_WRITE_CLASS.key(), JsonLinesErrorTableWriter.class.getName());
    return props;
  }

  /** The record keys of the blob records quarantined in the error table at {@code errorTablePath}. */
  public static List<String> quarantinedRecordKeys(String errorTablePath) throws IOException {
    List<String> recordKeys = new ArrayList<>();
    for (Map<String, Object> event : JsonLinesErrorTableWriter.read(errorTablePath)) {
      assertEquals("CUSTOM_TRANSFORMER_FAILURE", event.get("reason"), "blob content faults use ordinary transformer routing");
      Map<?, ?> payload = (Map<?, ?>) event.get("payload");
      recordKeys.add((String) payload.get("record_key"));
    }
    return recordKeys;
  }

  /** The error messages of the blob records quarantined in the error table at {@code errorTablePath}, by record key. */
  public static Map<String, String> quarantinedErrors(String errorTablePath) throws IOException {
    Map<String, String> errors = new LinkedHashMap<>();
    for (Map<String, Object> event : JsonLinesErrorTableWriter.read(errorTablePath)) {
      assertEquals("CUSTOM_TRANSFORMER_FAILURE", event.get("reason"), "blob content faults use ordinary transformer routing");
      Map<?, ?> payload = (Map<?, ?>) event.get("payload");
      errors.put((String) payload.get("record_key"), (String) payload.get("error"));
    }
    return errors;
  }

  /** Writes objects of {@link #BUCKET} (key to size) with {@link #fileBytes} and returns key to bytes, in order. */
  public Map<String, byte[]> putAll(Map<String, Integer> files, List<S3EventsTableFixture.Event> events) throws IOException {
    Map<String, byte[]> written = new LinkedHashMap<>();
    for (Map.Entry<String, Integer> file : files.entrySet()) {
      byte[] bytes = fileBytes(file.getKey(), file.getValue());
      events.add(put(file.getKey(), bytes));
      written.put(file.getKey(), bytes);
    }
    return written;
  }

  /** Writes the objects and announces them in one events commit; returns key to bytes. */
  public Map<String, byte[]> putAndAnnounce(SparkSession spark, Map<String, Integer> files) throws IOException {
    List<S3EventsTableFixture.Event> events = new ArrayList<>();
    Map<String, byte[]> written = putAll(files, events);
    announce(spark, events);
    return written;
  }

  /** Appends {@code events} to the events table as one commit. */
  public void announce(SparkSession spark, List<S3EventsTableFixture.Event> events) {
    S3EventsTableFixture.appendEvents(spark, eventsPath, events);
  }

  /**
   * The blob job's properties: the job file {@value #PROPS_RESOURCE}, the events table, the {@code bucketfs} URL prefix, the inline
   * threshold. No table path: the blob source reads none, the table is the streamer's target base path.
   */
  public TypedProperties jobProperties(long inlineMaxBytes) throws IOException {
    TypedProperties props = new TypedProperties();
    try (InputStream in = BlobJobHarness.class.getResourceAsStream(PROPS_RESOURCE)) {
      if (in == null) {
        throw new IOException("missing " + PROPS_RESOURCE);
      }
      props.load(in);
    }
    props.setProperty(HoodieIncrSourceConfig.HOODIE_SRC_BASE_PATH.key(), eventsPath);
    props.setProperty(S3EventsHoodieIncrSourceConfig.S3_FS_PREFIX.key(), BucketFolderFileSystem.SCHEME);
    props.setProperty(BlobTableConfig.INLINE_MAX_BYTES.key(), String.valueOf(inlineMaxBytes));
    return props;
  }

  /** HoodieStreamer configuration of the blob job: COW, {@link UnstructuredDataSource}, UPSERT, with commit-time ordering. */
  public static HoodieStreamer.Config streamerConfig(String tableBasePath) {
    return streamerConfig(tableBasePath, HoodieTableType.COPY_ON_WRITE);
  }

  /** The same blob job with the stock streamer table type selected by the caller. */
  public static HoodieStreamer.Config streamerConfig(String tableBasePath, HoodieTableType tableType) {
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.targetBasePath = tableBasePath;
    cfg.targetTableName = "blob_table";
    cfg.tableType = tableType.name();
    cfg.sourceClassName = UnstructuredDataSource.class.getName();
    cfg.transformerClassNames = java.util.Collections.singletonList(BlobQuarantineTransformer.class.getName());
    cfg.operation = WriteOperationType.UPSERT;
    cfg.sourceOrderingFields = null;
    cfg.recordMergeMode = RecordMergeMode.COMMIT_TIME_ORDERING;
    Triple<RecordMergeMode, String, String> mergeCfgs = HoodieTableConfig.inferMergingConfigsForWrites(
        cfg.recordMergeMode, cfg.payloadClassName, cfg.recordMergeStrategyId, cfg.sourceOrderingFields,
        HoodieTableVersion.current());
    cfg.recordMergeMode = mergeCfgs.getLeft();
    cfg.payloadClassName = mergeCfgs.getMiddle();
    cfg.recordMergeStrategyId = mergeCfgs.getRight();
    return cfg;
  }

  /** Deterministic bytes of the given size starting with the magic number of the file's type. */
  public static byte[] fileBytes(String name, int size) {
    byte[] bytes = new byte[size];
    new Random(name.hashCode()).nextBytes(bytes);
    byte[] magic;
    switch (BlobFileNames.extension(name)) {
      case "pdf":
        magic = "%PDF-1.4\n".getBytes(StandardCharsets.US_ASCII);
        break;
      case "png":
        magic = new byte[] {(byte) 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'};
        break;
      case "jpg":
        magic = new byte[] {(byte) 0xFF, (byte) 0xD8, (byte) 0xFF, (byte) 0xE0};
        break;
      case "mp4":
        magic = new byte[] {0, 0, 0, 0x18, 'f', 't', 'y', 'p', 'm', 'p', '4', '2'};
        break;
      default:
        magic = new byte[0];
    }
    System.arraycopy(magic, 0, bytes, 0, Math.min(magic.length, size));
    return bytes;
  }

  /** Writes a local file, creating its folders. */
  public static void writeFile(Path path, byte[] bytes) throws IOException {
    Files.createDirectories(path.getParent());
    Files.write(path, bytes);
  }

  public static String sha256Hex(byte[] bytes) {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
      StringBuilder hex = new StringBuilder();
      for (byte b : digest) {
        hex.append(String.format("%02x", b));
      }
      return hex.toString();
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
