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

package org.apache.hudi.metadata;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.metadata.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.RECORD_INDEX_AVERAGE_RECORD_SIZE;

/**
 * Estimates the bytes an entry takes in the global record index, so that its file group count follows the tables it
 * indexes rather than a fixed entry size.
 *
 * <p>An entry is estimated as {@code v + keyFactor * r * keyBytes}:
 * <ul>
 *   <li>{@code keyBytes} and {@code r} are the average size and the compression ratio of the record keys, from the
 *   {@code _hoodie_record_key} column of the footers of a sample of the base files.</li>
 *   <li>{@code keyFactor} scales the compressed key size of the base files up to the size the keys take in the record
 *   index HFiles, which compress them with GZ in key order.</li>
 *   <li>{@code v} is the size the value and the HFile framing of an entry add to its key once compressed. It depends on
 *   how often neighbouring entries repeat their partition path, file id and instant, so it is evaluated from the file
 *   slices: sample entries are serialized as the record index serializes them, ordered by random keys as hashed keys
 *   are, and compressed in HFile-sized GZ blocks with and without their values.</li>
 * </ul>
 *
 * <p>A table without meta fields has no record key column in its footers, so its entries, like those of a table whose
 * footers do not report the key size, are estimated at {@link HoodieTableMetadataUtil#RECORD_INDEX_AVERAGE_RECORD_SIZE}
 * bytes.
 */
@Slf4j
public class HoodieRecordIndexEntrySizeEstimator {

  // Parquet stores each BYTE_ARRAY value after its length, as a 4-byte integer, which the footer counts in the size of
  // the column.
  static final int PARQUET_BYTE_ARRAY_LENGTH_BYTES = 4;

  // Most base files whose footers are read for the record key size and compression ratio.
  static final int SAMPLE_FILE_COUNT = 100;

  // Entries serialized to evaluate the compressed size of a value.
  static final int SAMPLE_ENTRY_COUNT = 100_000;

  // Fixed seed, so that the same file slices always evaluate to the same value size.
  private static final long SAMPLE_SEED = 42L;

  /**
   * Estimates the size of a global record index entry for the table, given its latest file slices.
   *
   * @param engineContext  engine context to read the base file footers with
   * @param metaClient     meta client of the data table
   * @param fileSlices     latest file slices of the data table, which the record index is initialized from
   * @param fileIdEncoding encoding of the file ids the record index stores
   * @param keyFactor      factor that scales the compressed key size of the base files up to that of the index
   * @return the estimated bytes per entry
   */
  public static int estimateEntrySize(HoodieEngineContext engineContext, HoodieTableMetaClient metaClient,
                                      List<FileSliceAndPartition> fileSlices, int fileIdEncoding, double keyFactor) {
    if (!metaClient.getTableConfig().populateMetaFields()) {
      log.info("Estimating record index entries of table {} at {} bytes, as it has no record key column to size its keys from",
          metaClient.getBasePath(), RECORD_INDEX_AVERAGE_RECORD_SIZE);
      return RECORD_INDEX_AVERAGE_RECORD_SIZE;
    }
    Option<KeyStats> keyStats = readKeyStatsFromFooters(engineContext, metaClient, fileSlices);
    if (!keyStats.isPresent()) {
      log.info("Estimating record index entries of table {} at {} bytes, as no base file footer reports the record key size",
          metaClient.getBasePath(), RECORD_INDEX_AVERAGE_RECORD_SIZE);
      return RECORD_INDEX_AVERAGE_RECORD_SIZE;
    }
    double valueBytes = evaluateValueBytes(fileSlices, fileIdEncoding);
    int entrySize = entrySize(valueBytes, keyStats.get(), keyFactor);
    log.info("Estimating record index entries of table {} at {} bytes: {} bytes of value and {} key bytes compressing to a ratio of {}",
        metaClient.getBasePath(), entrySize, valueBytes, keyStats.get().getKeyBytes(), keyStats.get().getCompressionRatio());
    return entrySize;
  }

  static int entrySize(double valueBytes, KeyStats keyStats, double keyFactor) {
    return Math.max(1, (int) Math.ceil(valueBytes + keyFactor * keyStats.getCompressionRatio() * keyStats.getKeyBytes()));
  }

  /**
   * Reads the average size and the compression ratio of the record keys from the {@code _hoodie_record_key} column of
   * the footers of up to {@link #SAMPLE_FILE_COUNT} of the Parquet base files of the given file slices.
   *
   * @return the record key stats, or empty when no footer reports the size of plainly encoded keys
   */
  static Option<KeyStats> readKeyStatsFromFooters(HoodieEngineContext engineContext, HoodieTableMetaClient metaClient,
                                                  List<FileSliceAndPartition> fileSlices) {
    List<String> parquetBaseFiles = fileSlices.stream()
        .map(fileSliceAndPartition -> fileSliceAndPartition.getFileSlice().getBaseFile())
        .filter(baseFile -> baseFile.isPresent() && baseFile.get().getPath().endsWith(HoodieFileFormat.PARQUET.getFileExtension()))
        .map(baseFile -> baseFile.get().getPath())
        .sorted()
        .collect(Collectors.toList());
    if (parquetBaseFiles.isEmpty()) {
      return Option.empty();
    }
    List<String> sampledFiles = spreadSample(parquetBaseFiles, SAMPLE_FILE_COUNT);
    List<KeyColumnSizes> columnSizes = engineContext.map(sampledFiles,
        path -> readRecordKeyColumnSizes(metaClient.getStorage(), new StoragePath(path)), sampledFiles.size());
    return KeyStats.of(columnSizes);
  }

  /**
   * Returns the uncompressed size, the compressed size and the value count of the record key column of a Parquet file,
   * or zeros when the footer cannot be read or its size does not follow the length of the keys.
   */
  private static KeyColumnSizes readRecordKeyColumnSizes(HoodieStorage storage, StoragePath path) {
    List<HoodieColumnRangeMetadata<Comparable>> columnStats;
    try {
      columnStats = HoodieIOFactory.getIOFactory(storage)
          .getFileFormatUtils(HoodieFileFormat.PARQUET)
          .readColumnStatsFromMetadata(storage, path, Collections.singletonList(HoodieRecord.RECORD_KEY_METADATA_FIELD), HoodieIndexVersion.V1);
    } catch (HoodieIOException e) {
      log.warn("Skipping the footer of {} in the record key size estimate, as it cannot be read", path, e);
      return KeyColumnSizes.NONE;
    }
    return columnStats.stream()
        .filter(stats -> HoodieRecord.RECORD_KEY_METADATA_FIELD.equals(stats.getColumnName()) && stats.getValueCount() > 0)
        .filter(HoodieRecordIndexEntrySizeEstimator::isPlainEncodedSize)
        .findFirst()
        .map(stats -> new KeyColumnSizes(stats.getTotalUncompressedSize(), stats.getTotalSize(), stats.getValueCount()))
        .orElse(KeyColumnSizes.NONE);
  }

  /**
   * Whether the column size follows the length of the keys, as it does for plainly encoded values. A dictionary encoded
   * column of repeated keys takes far fewer bytes per value than its keys are long, which shows against the length of
   * its smallest and largest keys.
   */
  static boolean isPlainEncodedSize(HoodieColumnRangeMetadata<Comparable> stats) {
    if (stats.getMinValue() == null || stats.getMaxValue() == null) {
      return true;
    }
    int shorterBoundKeyBytes = Math.min(stats.getMinValue().toString().getBytes(StandardCharsets.UTF_8).length,
        stats.getMaxValue().toString().getBytes(StandardCharsets.UTF_8).length);
    double keyBytesPerValue = (double) stats.getTotalUncompressedSize() / stats.getValueCount() - PARQUET_BYTE_ARRAY_LENGTH_BYTES;
    return keyBytesPerValue >= shorterBoundKeyBytes / 2.0;
  }

  /**
   * Evaluates the size the value and the HFile framing of an entry add to its key once compressed. Each of
   * {@link #SAMPLE_ENTRY_COUNT} sample entries points to a file slice drawn in proportion to its size, as the records of
   * the record index do, and gets a random key, so that the entries meet in the order hashed record keys put them in.
   * The entries are compressed in HFile-sized GZ blocks as the record index HFiles compress them, once
   * whole and once with only their keys, and the difference is spread over the entries.
   *
   * @return the compressed bytes per entry of the values and the framing, or 0 when there are no file slices
   */
  static double evaluateValueBytes(List<FileSliceAndPartition> fileSlices, int fileIdEncoding) {
    if (fileSlices.isEmpty()) {
      return 0;
    }
    long[] cumulativeSizes = new long[fileSlices.size()];
    long totalSize = 0;
    for (int i = 0; i < fileSlices.size(); i++) {
      totalSize += Math.max(1L, fileSlices.get(i).getFileSlice().getTotalFileSize());
      cumulativeSizes[i] = totalSize;
    }
    // Only the drawn file slices are serialized, so a table of many file slices costs at most one value per sample.
    Map<Integer, byte[]> values = new HashMap<>();
    Random random = new Random(SAMPLE_SEED);
    List<HoodieIndexEntrySample> entries = new ArrayList<>(SAMPLE_ENTRY_COUNT);
    for (int i = 0; i < SAMPLE_ENTRY_COUNT; i++) {
      long position = (long) (random.nextDouble() * totalSize) + 1;
      int index = Arrays.binarySearch(cumulativeSizes, position);
      byte[] key = new UUID(random.nextLong(), random.nextLong()).toString().getBytes(StandardCharsets.UTF_8);
      byte[] value = values.computeIfAbsent(index >= 0 ? index : -index - 1,
          drawn -> serializeValue(fileSlices.get(drawn).getPartitionPath(), fileSlices.get(drawn).getFileSlice(), fileIdEncoding));
      entries.add(new HoodieIndexEntrySample(key, value));
    }
    entries.sort(HoodieIndexEntrySample.HFILE_KEY_ORDER);
    return (double) (HoodieIndexEntrySample.compressedSize(entries, true) - HoodieIndexEntrySample.compressedSize(entries, false))
        / SAMPLE_ENTRY_COUNT;
  }

  /**
   * Serializes the record index value of a file slice the way the record index HFile writer stores it, without the key.
   */
  private static byte[] serializeValue(String partitionPath, FileSlice fileSlice, int fileIdEncoding) {
    return HoodieIndexEntrySample.serializeValue(HoodieMetadataPayload.createRecordIndexUpdate("", partitionPath,
        fileSlice.getFileId(), HoodieMetadataPayload.parseRecordIndexInstantTime(fileSlice.getBaseInstantTime()), fileIdEncoding));
  }

  /**
   * Picks up to {@code count} of the items, spread evenly over the list.
   */
  static <T> List<T> spreadSample(List<T> items, int count) {
    if (items.size() <= count) {
      return items;
    }
    return IntStream.range(0, count)
        .mapToObj(i -> items.get((int) ((long) i * items.size() / count)))
        .collect(Collectors.toList());
  }

  /**
   * Uncompressed size, compressed size and value count of the record key column of one base file footer.
   */
  @Value
  static class KeyColumnSizes implements Serializable {
    private static final long serialVersionUID = 1L;
    static final KeyColumnSizes NONE = new KeyColumnSizes(0, 0, 0);

    long uncompressedSize;
    long compressedSize;
    long valueCount;
  }

  /**
   * Average size and compression ratio of the record keys, as the base file footers report them.
   */
  @Getter
  @AllArgsConstructor(access = AccessLevel.PACKAGE)
  static class KeyStats {
    private final double keyBytes;
    private final double compressionRatio;

    /**
     * Sums the uncompressed sizes, compressed sizes and value counts of the record key column across footers.
     *
     * @return the stats, or empty when no footer counted a key
     */
    static Option<KeyStats> of(List<KeyColumnSizes> columnSizes) {
      long uncompressed = columnSizes.stream().mapToLong(KeyColumnSizes::getUncompressedSize).sum();
      long compressed = columnSizes.stream().mapToLong(KeyColumnSizes::getCompressedSize).sum();
      long valueCount = columnSizes.stream().mapToLong(KeyColumnSizes::getValueCount).sum();
      if (valueCount == 0 || uncompressed == 0) {
        return Option.empty();
      }
      double keyBytes = (double) uncompressed / valueCount - PARQUET_BYTE_ARRAY_LENGTH_BYTES;
      return keyBytes > 0 ? Option.of(new KeyStats(keyBytes, (double) compressed / uncompressed)) : Option.empty();
    }
  }
}
