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

package org.apache.hudi.utilities.unstructured.embedding.input;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.testutils.HandleCountingFileSystem;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageContentTypes;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWriterTestExtension;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The input store: {@link PreparedInputs#prepare} closes the payload stream it opens and, through {@link PayloadWriter}, the
 * payload file it writes (in the background, done by {@link PreparedInputs#awaitWrites}), on the inline and out-of-line main
 * paths and when the payload or the file fails; a failed out-of-line write leaves no file under the embedding table's blob folder.
 */
@ExtendWith(OutOfLineWriterTestExtension.class)
class TestPreparedInputsHandleRelease {

  private static final long THRESHOLD = 64;
  private static final String INSTANT = "20261018120000000";
  private static final String JPEG = ImageContentTypes.JPEG;

  @TempDir
  Path tempDir;

  private Path table;
  private Path payloadFile;
  private Configuration conf;

  @BeforeEach
  void setUp() throws IOException {
    HandleCountingFileSystem.reset();
    conf = HandleCountingFileSystem.install(new Configuration());
    table = tempDir.resolve("embedding");
    Files.createDirectories(table);
    payloadFile = tempDir.resolve("frames").resolve("frame.bin");
    Files.createDirectories(payloadFile.getParent());
  }

  @AfterEach
  void resetRegistry() {
    HandleCountingFileSystem.reset();
  }

  @Test
  void mainPathClosesThePayloadAndTheStoredFile() throws IOException {
    byte[] small = bytes(32);
    PreparedInputs inputs = inputs();
    Row inline = inputs.prepare("row-0", "p1", JPEG, payload(small), small.length);
    assertArrayEquals(small, (byte[]) inline.getAs(HoodieSchema.Blob.INLINE_DATA_FIELD));
    byte[] large = bytes(500);
    Row outOfLine = inputs.prepare("row-1", "p1", JPEG, payload(large), large.length);
    inputs.awaitWrites();
    inputs.close();
    String externalPath = ((Row) outOfLine.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE)).getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH);
    assertEquals(3, HandleCountingFileSystem.opened(), "two payload reads and one stored file");
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "streams left open by the main path");
    assertEquals(Collections.singletonList(externalPath), blobFiles());
    assertArrayEquals(large, Files.readAllBytes(table.resolve(externalPath)));
  }

  @Test
  void payloadLongerThanDeclaredClosesBothStreamsAndLeavesNoFile() throws IOException {
    byte[] small = bytes(40);
    PreparedInputs first = inputs();
    BlobBytesSupplier smallPayload = payload(small);
    assertThrows(IllegalArgumentException.class, () -> first.prepare("row-0", "p1", JPEG, smallPayload, 32));
    byte[] large = bytes(500);
    PreparedInputs second = inputs();
    BlobBytesSupplier shortPayload = payload(large);
    assertThrows(IllegalArgumentException.class, () -> second.prepare("row-1", "p1", JPEG, shortPayload, 400));
    PreparedInputs third = inputs();
    BlobBytesSupplier longPayload = payload(large);
    assertThrows(IllegalArgumentException.class, () -> third.prepare("row-2", "p1", JPEG, longPayload, 600));
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "streams left open by a payload of the wrong size");
    assertEquals(Collections.emptyList(), blobFiles(), "files left by a payload of the wrong size");
  }

  @Test
  void failedPayloadReadClosesBothStreamsAndLeavesNoFile() throws IOException {
    byte[] small = bytes(32);
    BlobBytesSupplier smallPayload = payload(small);
    HandleCountingFileSystem.inject(payloadFile, HandleCountingFileSystem.Fault.READ, 1);
    PreparedInputs first = inputs();
    assertThrows(HoodieIOException.class, () -> first.prepare("row-0", "p1", JPEG, smallPayload, small.length));
    byte[] large = bytes(500);
    BlobBytesSupplier largePayload = payload(large);
    HandleCountingFileSystem.inject(payloadFile, HandleCountingFileSystem.Fault.READ, 1);
    PreparedInputs second = inputs();
    assertThrows(HoodieIOException.class, () -> second.prepare("row-1", "p1", JPEG, largePayload, large.length));
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "streams left open by a failed payload read");
    assertEquals(Collections.emptyList(), blobFiles(), "files left by a failed payload read");
  }

  @Test
  void failedStoredFileCloseClosesThePayloadAndLeavesNoFile() throws IOException {
    byte[] large = bytes(500);
    BlobBytesSupplier largePayload = payload(large);
    HandleCountingFileSystem.inject(table, HandleCountingFileSystem.Fault.CLOSE_WRITE, 1);
    PreparedInputs inputs = inputs();
    inputs.prepare("row-1", "p1", JPEG, largePayload, large.length);
    // written in the background: the failure is the task's at its barrier
    assertThrows(EmbeddingPayloadWriteException.class, inputs::awaitWrites);
    inputs.close();
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "streams left open by a failed stored-file close");
    assertEquals(Collections.emptyList(), blobFiles(), "files left by a failed stored-file close");
  }

  private PreparedInputs inputs() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_BYTES.key(), String.valueOf(THRESHOLD));
    return new PreparedInputs(props, INSTANT, new PayloadDestination(HadoopFSUtils.getStorageConf(conf),
        HandleCountingFileSystem.url(table), new TypedProperties()));
  }

  /** Writes {@code content} as the payload file and opens it through the blob source, as a frame source opens stored bytes. */
  private BlobBytesSupplier payload(byte[] content) throws IOException {
    Files.write(payloadFile, content);
    BlobBytesSource source = new BlobBytesSource(HadoopFSUtils.getStorageConf(conf));
    Row reference = new GenericRowWithSchema(new Object[] {HandleCountingFileSystem.url(payloadFile), null, null, false},
        PreparedInputs.REFERENCE_STRUCT);
    Row blob = new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.OUT_OF_LINE, null, reference}, PreparedInputs.BLOB_STRUCT);
    return () -> source.open(blob);
  }

  /** Every file under the embedding table's blob folder, relative to the table. */
  private List<String> blobFiles() throws IOException {
    Path blobFolder = table.resolve(".hoodie").resolve(".blob");
    if (!Files.exists(blobFolder)) {
      return Collections.emptyList();
    }
    try (Stream<Path> files = Files.walk(blobFolder)) {
      return files.filter(Files::isRegularFile).map(file -> table.relativize(file).toString()).sorted().collect(Collectors.toList());
    }
  }

  private static byte[] bytes(int size) {
    byte[] bytes = new byte[size];
    Arrays.fill(bytes, (byte) 3);
    return bytes;
  }
}
