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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.AbstractTableFileSystemView;
import org.apache.hudi.common.util.Base64CodecUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType.INSTANT_TIME;

/**
 * Utils class for performing various log file reading operations.
 */
public class LogReaderUtils {

  /**
   * Gets a map of log files which were created by commits with instant timestamps that are less than or equal to the
   * maxCommitInstantTime. All other log files will be filtered out. For each log file maps to a list of commit instant
   * times that are associated with each block that is found in the log file.
   *
   * @param metaClient Hoodie table meta client
   * @param fsView Hoodie file system view
   * @param partitionPaths list of partition paths to fetch log files from. for MDT, this should be "files" or
   *                      MetadataPartitionType.FILES.partitionPath()
   * @param maxCommitInstantTime the max commit which created the log files returned
   * @param engineContext Engine context
   * @return map of log file -> associated commit time for each block in the log file
   */
  public static Map<HoodieLogFile, List<String>> getAllLogFilesWithMaxCommit(HoodieTableMetaClient metaClient,
                                                                             AbstractTableFileSystemView fsView,
                                                                             List<String> partitionPaths,
                                                                             String maxCommitInstantTime,
                                                                             HoodieEngineContext engineContext) {
    engineContext.setJobStatus("LogReaderUtils",
        String.format("Getting list of log files in %s partition(s)", partitionPaths.size()));

    List<HoodieLogFile> logFiles = engineContext.flatMap(partitionPaths, partitionPath -> fsView
        .getLatestMergedFileSlicesBeforeOrOn(partitionPath, maxCommitInstantTime)
        .flatMap(FileSlice::getLogFiles),
        Math.max(partitionPaths.size(), 1));

    // get completion time of the max commit instant
    String maxCommitCompletionTime = metaClient.getActiveTimeline().filterCompletedInstants()
        .findInstantsAfterOrEquals(maxCommitInstantTime, 1)
        .filter(instant -> instant.requestedTime().equals(maxCommitInstantTime))
        .getInstants()
        .stream()
        .map(instant -> instant.getCompletionTime())
        .findFirst().orElse(maxCommitInstantTime);

    // filter out all commits completed after the max commit completion time
    HoodieTimeline filteredTimeline = fsView.getTimeline()
        .findInstantsModifiedBeforeOrEqualsByCompletionTime(maxCommitCompletionTime);


    engineContext.setJobStatus("LogReaderUtils",
        String.format("Getting log file map for %s partition(s)", partitionPaths.size()));
    Map<HoodieLogFile, List<String>> logFilesWithMaxCommit = engineContext.mapToPair(logFiles, logFile -> {
      // read all blocks within the log file and find the commit associated with each log block
      List<String> blocksWithinLogFile = new ArrayList<>();
      try (HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(metaClient, logFile, null)) {
        while (reader.hasNext()) {
          HoodieLogBlock block = reader.next();
          String logBlockInstantTime = block.getLogBlockHeader().get(INSTANT_TIME);
          // check if the log file contains a block created by a commit that is older than or equal to max commit
          // use containsOrBeforeTimelineStarts to handle cases where the log block's instant time is in archived timeline
          if (filteredTimeline.containsOrBeforeTimelineStarts(logBlockInstantTime)) {
            blocksWithinLogFile.add(logBlockInstantTime);
          }
        }
      }

      return Pair.of(logFile, blocksWithinLogFile);
    }, Math.max(logFiles.size(), 1));

    logFilesWithMaxCommit.values().removeIf(List::isEmpty);

    return logFilesWithMaxCommit;
  }

  /**
   * Encodes a list of record positions in long type.
   * <p>
   * The encoding applies the Base64 codec ({@link java.util.Base64} in Java implementation) on
   * the bytes generated from serializing {@link Roaring64NavigableMap} bitmap, which contains
   * the list of record positions in long type, using the portable
   * format.
   *
   * @param positions A list of long-typed positions.
   * @return A string of Base64-encoded bytes ({@link java.util.Base64} in Java implementation)
   * generated from serializing {@link Roaring64NavigableMap} bitmap using the portable format.
   * @throws IOException upon I/O error.
   */
  public static String encodePositions(Set<Long> positions) throws IOException {
    Roaring64NavigableMap positionBitmap = new Roaring64NavigableMap();
    positions.forEach(positionBitmap::add);
    return encodePositions(positionBitmap);
  }

  /**
   * Encodes the {@link Roaring64NavigableMap} bitmap containing the record positions.
   * <p>
   * The encoding applies the Base64 codec ({@link java.util.Base64} in Java implementation) on
   * the bytes generated from serializing {@link Roaring64NavigableMap} bitmap using the portable
   * format.
   *
   * @param positionBitmap {@link Roaring64NavigableMap} bitmap containing the record positions.
   * @return A string of Base64-encoded bytes ({@link java.util.Base64} in Java implementation)
   * generated from serializing {@link Roaring64NavigableMap} bitmap using the portable format.
   * @throws IOException upon I/O error.
   */
  public static String encodePositions(Roaring64NavigableMap positionBitmap) throws IOException {
    positionBitmap.runOptimize();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    positionBitmap.serializePortable(dos);
    return Base64CodecUtil.encode(baos.toByteArray());
  }

  /**
   * Encodes the record positions as an integer count followed by unsigned varlong positions,
   * preserving the input order.
   *
   * @param positions record positions in file read order.
   * @return Base64-encoded bytes containing the list size followed by varlong positions,
   * or {@link Option#empty()} if any position is invalid.
   * @throws IOException upon I/O error.
   */
  public static Option<String> encodePositionsAsLongList(List<Long> positions) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(Integer.BYTES + positions.size());
    DataOutputStream dos = new DataOutputStream(baos);
    dos.writeInt(positions.size());
    for (Long position : positions) {
      if (!HoodieRecordLocation.isPositionValid(position)) {
        return Option.empty();
      }
      writeUnsignedVarLong(baos, position);
    }
    return Option.of(Base64CodecUtil.encode(baos.toByteArray()));
  }

  /**
   * Decodes the {@link HeaderMetadataType#RECORD_POSITIONS} block header into record positions.
   *
   * @param content A string of Base64-encoded bytes ({@link java.util.Base64} in Java
   *                implementation) generated from serializing {@link Roaring64NavigableMap}
   *                bitmap using the portable format.
   * @return A {@link Roaring64NavigableMap} bitmap containing the record positions in long type.
   * @throws IOException upon I/O error.
   */
  public static Roaring64NavigableMap decodeRecordPositionsHeader(String content) throws IOException {
    Roaring64NavigableMap positionBitmap = new Roaring64NavigableMap();
    ByteArrayInputStream bais = new ByteArrayInputStream(Base64CodecUtil.decode(content));
    DataInputStream dis = new DataInputStream(bais);
    positionBitmap.deserializePortable(dis);
    return positionBitmap;
  }

  /**
   * Decodes native-log record positions written as an ordered unsigned varlong list.
   *
   * @param content Base64-encoded bytes containing the list size followed by varlong positions.
   * @return record positions in file read order.
   * @throws IOException upon I/O error or invalid payload shape.
   */
  public static List<Long> decodeRecordPositionsLongList(String content) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(Base64CodecUtil.decode(content));
    DataInputStream dis = new DataInputStream(bais);
    int size = dis.readInt();
    if (size < 0) {
      throw new IOException("Invalid negative record position count " + size);
    }
    List<Long> positions = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      long position = readUnsignedVarLong(bais);
      if (!HoodieRecordLocation.isPositionValid(position)) {
        throw new IOException("Invalid record position " + position);
      }
      positions.add(position);
    }
    if (bais.available() != 0) {
      throw new IOException("Invalid record positions payload with " + bais.available() + " trailing bytes");
    }
    return positions;
  }

  /**
   * Writes an unsigned long by splitting it into 7-bit groups from low bits to high bits.
   * The most significant bit of each byte is the continuation flag: {@code 1} means more
   * bytes follow and {@code 0} marks the final byte.
   */
  private static void writeUnsignedVarLong(ByteArrayOutputStream out, long value) {
    while ((value & 0xFFFFFFFFFFFFFF80L) != 0) {
      out.write(((int) value & 0x7F) | 0x80);
      value >>>= 7;
    }
    out.write((int) value & 0x7F);
  }

  /**
   * Reads an unsigned long written by {@link #writeUnsignedVarLong(ByteArrayOutputStream, long)}
   * by rebuilding the value from 7-bit groups in low-to-high order.
   */
  private static long readUnsignedVarLong(ByteArrayInputStream in) throws IOException {
    long value = 0;
    int shift = 0;
    while (shift < Long.SIZE) {
      int currentByte = in.read();
      if (currentByte < 0) {
        throw new EOFException("Unexpected EOF while reading record position");
      }
      value |= (long) (currentByte & 0x7F) << shift;
      if ((currentByte & 0x80) == 0) {
        return value;
      }
      shift += 7;
    }
    throw new IOException("Invalid unsigned varlong for record position");
  }
}
