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

package org.apache.hudi.utilities.testutils;

import org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Reads the footers of a local table's base files: which codec its column chunks use and whether a record-key bloom filter is stored. */
public final class ParquetFooters {

  private ParquetFooters() {
  }

  /** Every Parquet base file of the table at {@code basePath} (a {@code file:} URI or path), outside {@code .hoodie}. */
  public static List<Path> baseFiles(String basePath) throws IOException {
    try (Stream<Path> walk = Files.walk(Paths.get(URI.create(basePath)))) {
      return walk.filter(Files::isRegularFile).filter(p -> p.toString().endsWith(".parquet") && !p.toString().contains("/.hoodie/"))
          .collect(Collectors.toList());
    }
  }

  /**
   * Asserts that every column chunk of every base file of the table is compressed with {@code codec}, and that each file's footer
   * carries a record-key bloom filter exactly when {@code bloomFilter} is true.
   */
  public static void assertBaseFiles(Configuration conf, String basePath, CompressionCodecName codec, boolean bloomFilter)
      throws IOException {
    List<Path> files = baseFiles(basePath);
    assertFalse(files.isEmpty(), "no base file under " + basePath);
    for (Path file : files) {
      try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(file.toUri()), conf))) {
        ParquetMetadata footer = reader.getFooter();
        for (BlockMetaData block : footer.getBlocks()) {
          for (ColumnChunkMetaData column : block.getColumns()) {
            assertEquals(codec, column.getCodec(), file + " " + column.getPath());
          }
        }
        assertEquals(bloomFilter, footer.getFileMetaData().getKeyValueMetaData()
            .containsKey(HoodieBloomFilterWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY), "bloom filter in " + file);
      }
    }
  }
}
