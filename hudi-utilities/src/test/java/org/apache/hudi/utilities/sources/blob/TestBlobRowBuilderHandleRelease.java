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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.sources.helpers.CloudObjectMetadata;
import org.apache.hudi.utilities.testutils.HandleCountingFileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The S3 read of the blob source: {@link BlobRowBuilder} opens a source object only to read an inline-size one whole, and
 * releases that stream on every path; its lazy iterator holds no stream between rows, so a consumer that stops early leaks none.
 */
public class TestBlobRowBuilderHandleRelease {

  @TempDir
  Path tempDir;

  @BeforeEach
  @AfterEach
  public void resetRegistry() {
    HandleCountingFileSystem.reset();
  }

  @Test
  public void mainPathReleasesTheInlineReadAndOpensNothingForAnOutOfLineObject() throws IOException {
    byte[] small = bytes(10);
    Row inline = builder().buildRow(object("small.pdf", small));
    Row blob = (Row) inline.get(BlobTableSchema.schema().fieldIndex(BlobTableSchema.BLOB));
    assertArrayEquals(small, (byte[]) blob.get(1));
    Row outOfLine = builder().buildRow(object("large.pdf", bytes(100)));
    assertNull(((Row) outOfLine.get(BlobTableSchema.schema().fieldIndex(BlobTableSchema.BLOB))).get(1));
    assertEquals(1, HandleCountingFileSystem.opened(), "only the inline-size object is read");
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "source streams left open by the main path");
  }

  @Test
  public void failedInlineReadReleasesTheSourceStream() throws IOException {
    CloudObjectMetadata small = object("small.pdf", bytes(10));
    HandleCountingFileSystem.inject(tempDir.resolve("small.pdf"), HandleCountingFileSystem.Fault.READ, 1);
    assertThrows(HoodieIOException.class, () -> builder().buildRow(small));
    assertEquals(1, HandleCountingFileSystem.opened());
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "source streams left open by a failed read");
  }

  @Test
  public void iteratorAbandonedAfterOneRowHoldsNoStream() throws IOException {
    Iterator<Row> rows = builder().build(Arrays.asList(object("a.pdf", bytes(5)), object("b.pdf", bytes(6)),
        object("c.pdf", bytes(7))).iterator());
    assertTrue(rows.hasNext());
    rows.next();
    // the consumer stops pulling here (Spark ends the task early)
    assertEquals(1, HandleCountingFileSystem.opened());
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "source streams held by an abandoned iterator");
  }

  private BlobRowBuilder builder() {
    TypedProperties props = new TypedProperties();
    props.setProperty(BlobTableConfig.INLINE_MAX_BYTES.key(), "16");
    return new BlobRowBuilder(props, HadoopFSUtils.getStorageConf(HandleCountingFileSystem.install(new Configuration())));
  }

  private CloudObjectMetadata object(String name, byte[] content) throws IOException {
    Path file = tempDir.resolve(name);
    Files.write(file, content);
    return new CloudObjectMetadata(HandleCountingFileSystem.url(file), content.length);
  }

  private static byte[] bytes(int size) {
    byte[] bytes = new byte[size];
    Arrays.fill(bytes, (byte) 7);
    return bytes;
  }
}
