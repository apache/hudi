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

package org.apache.hudi.utilities.unstructured.storage;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.utilities.testutils.HandleCountingFileSystem;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link OutOfLineWriter#write} closes the target stream on every path and removes the file of a write that failed, as its
 * contract says ("a failed stream or close removes the file best-effort"), whatever the stream failed with.
 */
public class TestOutOfLineWriterHandleRelease {

  private static final byte[] CONTENT = {1, 2, 3, 4, 5, 6, 7, 8};

  @TempDir
  Path tempDir;

  private Path table;
  private OutOfLineWriter writer;

  @BeforeEach
  public void createWriter() throws IOException {
    HandleCountingFileSystem.reset();
    table = tempDir.resolve("table");
    Files.createDirectories(table);
    String base = HandleCountingFileSystem.url(table);
    HoodieStorage storage = new HoodieHadoopStorage(new StoragePath(base),
        HadoopFSUtils.getStorageConf(HandleCountingFileSystem.install(new Configuration())));
    writer = new OutOfLineWriter(storage, base, "101", Option.empty());
  }

  @AfterEach
  public void resetRegistry() {
    HandleCountingFileSystem.reset();
  }

  @Test
  public void mainPathClosesTheTarget() throws IOException {
    String relative = writer.write("pdf", out -> out.write(CONTENT));
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "target streams left open by the main path");
    assertEquals(Collections.singletonList(relative), blobFiles());
    assertArrayEquals(CONTENT, Files.readAllBytes(table.resolve(relative)));
  }

  @Test
  public void failedStreamClosesAndRemovesTheTarget() throws IOException {
    assertThrows(IOException.class, () -> writer.write("pdf", out -> {
      out.write(CONTENT);
      throw new IOException("injected stream failure");
    }));
    assertThrows(IllegalStateException.class, () -> writer.write("pdf", out -> {
      out.write(CONTENT);
      throw new IllegalStateException("injected unchecked stream failure");
    }));
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "target streams left open by a failed stream");
    assertEquals(Collections.emptyList(), blobFiles(), "files left by a failed stream");
  }

  @Test
  public void failedCloseRemovesTheTarget() throws IOException {
    HandleCountingFileSystem.inject(table, HandleCountingFileSystem.Fault.CLOSE_WRITE, 1);
    assertThrows(IOException.class, () -> writer.write("pdf", out -> out.write(CONTENT)));
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "target streams left open by a failed close");
    assertEquals(Collections.emptyList(), blobFiles(), "files left by a failed close");
  }

  @Test
  public void errorFromTheStreamClosesTheTarget() {
    assertThrows(NoClassDefFoundError.class, () -> writer.write("pdf", out -> {
      out.write(CONTENT);
      throw new NoClassDefFoundError("injected: a codec class missing on the executor");
    }));
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "target streams left open by an Error of the stream");
  }

  @Test
  public void errorFromTheStreamRemovesThePartialFile() throws IOException {
    assertThrows(NoClassDefFoundError.class, () -> writer.write("pdf", out -> {
      out.write(CONTENT);
      throw new NoClassDefFoundError("injected: a codec class missing on the executor");
    }));
    assertEquals(Collections.emptyList(), blobFiles(), "partial out-of-line file left under .hoodie/.blob by a write whose stream "
        + "threw an Error (only IOException and RuntimeException remove it)");
  }

  /** Every file under the table's blob folder, relative to the table. */
  private List<String> blobFiles() throws IOException {
    Path blobFolder = table.resolve(".hoodie").resolve(".blob");
    if (!Files.exists(blobFolder)) {
      return Collections.emptyList();
    }
    try (Stream<Path> files = Files.walk(blobFolder)) {
      return files.filter(Files::isRegularFile).map(file -> table.relativize(file).toString()).sorted().collect(Collectors.toList());
    }
  }
}
