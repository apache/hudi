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

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.sources.blob.BlobRowBuilder;
import org.apache.hudi.utilities.sources.blob.BlobTableConfig;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.sources.helpers.CloudObjectMetadata;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobSourceChangedException;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The source's whole-file references retain embedding prep's existing listed-version check. */
@Tag("functional")
class TestPrepRoundSourceObjects extends SparkClientFunctionalTestHarness {

  /**
   * The rows the source's own {@link BlobRowBuilder} builds for objects above its inline threshold (an unmanaged reference to the
   * object itself, without offset or length), as prep
   * receives them: an object overwritten between listing and prep is refused as changed since it was listed, by modification time
   * or by size, and an unchanged one passes the same check.
   */
  @Test
  void inPlaceReferenceIsCheckedAgainstTheListedVersion(@TempDir Path folder) throws IOException {
    byte[] listed = "%PDF listed".getBytes(StandardCharsets.UTF_8);
    List<CloudObjectMetadata> objects = new ArrayList<>();
    for (String name : Arrays.asList("touched.pdf", "grown.pdf", "kept.pdf")) {
      Path file = Files.write(folder.resolve(name), listed);
      Files.setLastModifiedTime(file, FileTime.fromMillis(1000L));
      objects.add(new CloudObjectMetadata(file.toUri().toString(), listed.length));
    }
    TypedProperties props = new TypedProperties();
    // every object is above the threshold, so the builder references each in place
    props.setProperty(BlobTableConfig.INLINE_MAX_BYTES.key(), "0");
    StorageConfiguration<?> storageConf = HadoopFSUtils.getStorageConf(new Configuration());
    List<Row> listing = new ArrayList<>();
    new BlobRowBuilder(props, storageConf).build(objects.iterator()).forEachRemaining(listing::add);
    // overwritten after listing: a new modification time, and a larger object under the listed modification time
    Files.setLastModifiedTime(folder.resolve("touched.pdf"), FileTime.fromMillis(2000L));
    Files.write(folder.resolve("grown.pdf"), "%PDF overwritten, longer".getBytes(StandardCharsets.UTF_8));
    Files.setLastModifiedTime(folder.resolve("grown.pdf"), FileTime.fromMillis(1000L));

    BlobBytesSource source = new BlobBytesSource(storageConf);
    Map<String, Row> prepared = new TreeMap<>();
    for (Row out : spark().createDataFrame(listing, BlobTableSchema.schema()).collectAsList()) {
      prepared.put(out.getAs(BlobTableSchema.SOURCE_FILE_NAME), out);
    }
    for (String changed : Arrays.asList("touched.pdf", "grown.pdf")) {
      BlobSourceChangedException failure = assertThrows(BlobSourceChangedException.class, () -> checkListedVersion(source,
          prepared.get(changed)), changed);
      assertTrue(failure.getMessage().contains("changed since it was listed"), failure.getMessage());
    }
    checkListedVersion(source, prepared.get("kept.pdf"));
  }

  /** The version check prep runs before it opens a row's object: against the row's own listed modification time and size. */
  private static void checkListedVersion(BlobBytesSource source, Row row) throws IOException {
    assertEquals(1000L, (long) row.<Long>getAs(BlobTableSchema.SOURCE_MODIFICATION_TIME), "the listing's status call");
    source.checkVersion(row.getAs(BlobTableSchema.BLOB), row.getAs(BlobTableSchema.SOURCE_MODIFICATION_TIME),
        row.<Long>getAs(BlobTableSchema.SOURCE_FILE_SIZE));
  }
}
