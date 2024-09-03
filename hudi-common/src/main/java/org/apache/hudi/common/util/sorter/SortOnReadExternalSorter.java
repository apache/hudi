/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util.sorter;

import org.apache.hudi.common.fs.SizeAwareDataOutputStream;
import org.apache.hudi.common.util.BinaryUtil;
import org.apache.hudi.common.util.BufferedRandomAccessFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class SortOnReadExternalSorter<R extends Serializable> extends ExternalSorter<R> {

  private static final Logger LOG = LoggerFactory.getLogger(SortOnReadExternalSorter.class);

  private static final int DEFAULT_WRITE_BUFFER = 64 * 1024; // 64KB

  private static final int DEFAULT_READ_BUFFER = 64 * 1024; // 64KB

  private static final String SPILL_FILE_SUFFIX = "sorted";

  private final SortEngine sortEngine;

  private long currentMemorySize;

  private long totalMemorySize;

  private int currentMemoryEntryCount;

  private int totalEntryCount;

  // TODO: better sorter
  private CollectionSorter<List<R>> collectionSorter;

  private SortedFileManager fileManager = new SortedFileManager();

  // TODO: better serializeFunc
  private SerializeFunc<R> serializeFunc = new SerializeFunc<R>() {
    @Override
    public byte[] serialize(R record) {
      try {
        return SerializationUtils.serialize(record);
      } catch (IOException e) {
        LOG.error("Failed to serialize record: {}", record, e);
        throw new HoodieIOException("Failed to serialize record", e);
      }
    }

    @Override
    public R deserialize(byte[] bytes) {
      return SerializationUtils.deserialize(bytes);
    }
  };

  private List<R> memoryEntries;

  public SortOnReadExternalSorter(String baseFilePath, long maxMemorySize, Comparator<R> comparator, SizeEstimator<R> sizeEstimator, SortEngine sortEngine) throws IOException {
    super(baseFilePath, maxMemorySize, comparator, sizeEstimator);
    this.sortEngine = sortEngine;
    this.collectionSorter = (collection) -> {
      collection.sort(comparator);
      return collection;
    };
    this.memoryEntries = new LinkedList<>();
  }

  @Override
  public void closeSorter() {
    LOG.info("ExternalSorter2 closed, max memory size => {}, total memory size => {}, current memory size => {} \n"
            + " total entry count => {}, current memory entry count => {} \n"
            + " total sorted file count => {}, total sorted file size => {} \n"
            + " total time taken to insert and write record => {}", maxMemoryInBytes, totalMemorySize, currentMemorySize, totalEntryCount, currentMemoryEntryCount,
        fileManager.fileCount, fileManager.totalFileSize, getTimeTakenToInsertAndWriteRecord());
    fileManager.clear();
    memoryEntries.clear();
  }

  @Override
  protected void addInner(R record) {
    addRecord(record);
  }

  @Override
  protected void addAllInner(Iterator<R> inputRecords) {
    while (inputRecords.hasNext()) {
      addRecord(inputRecords.next());
    }
  }

  @Override
  protected void finishInner() {
    LOG.info("SortOnReadExternalSorter insert all completed, max memory size => {}, total memory size => {}, current memory size => {} \n"
            + " total entry count => {}, current memory entry count => {} \n"
            + " total sorted file count => {}, total sorted file size => {} \n"
            + " total time taken to insert and write record => {}", maxMemoryInBytes, totalMemorySize, currentMemorySize, totalEntryCount, currentMemoryEntryCount,
        fileManager.fileCount, fileManager.totalFileSize, getTimeTakenToInsertAndWriteRecord());
  }

  private void addRecord(R record) {
    memoryEntries.add(record);
    currentMemoryEntryCount++;
    totalEntryCount++;
    long sizeEstimate = sizeEstimator.sizeEstimate(record);
    currentMemorySize += sizeEstimate;
    totalMemorySize += sizeEstimate;
    if (currentMemorySize >= maxMemoryInBytes) {
      spill(memoryEntries);
      memoryEntries.clear();
      currentMemorySize = 0;
      currentMemoryEntryCount = 0;
    }
  }

  @Override
  protected ClosableIterator<R> getIteratorInner() {
    return mergeSort();
  }

  public SortMergeReader<R> mergeSort() {
    List<SortedEntryReader<R>> readers = new ArrayList<>(currentMemorySize == 0 ? fileManager.fileCount : fileManager.fileCount + 1);
    // TODO: more suitable batchSize
    if (fileManager.fileCount > 0) {
      long maxBatchSize = (maxMemoryInBytes - currentMemorySize) / fileManager.fileCount;
      for (int i = 0; i < fileManager.fileCount; i++) {
        readers.add(new SortedEntryFileReader(i, maxBatchSize, fileManager.files.get(i)));
      }
    }
    if (currentMemorySize > 0) {
      // sort the memory entries
      memoryEntries = collectionSorter.sort(memoryEntries);
      readers.add(new SortedEntryMemoryReader(fileManager.fileCount, memoryEntries));
    }

    SortMergeReader<R> sortMergeReader = SortMergeReader.create(sortEngine, readers, comparator);
    return sortMergeReader;
  }

  private void spill(List<R> unsortedEntries) {
    // spill the entries to disk
    List<R> sortedList = collectionSorter.sort(unsortedEntries);
    // write the sorted entries to disk
    CompletedCallback<File> fileToWriteCb;
    boolean success = false;
    try {
      fileToWriteCb = fileManager.createFileForWrite();
    } catch (IOException e) {
      LOG.error("Failed to create file for write", e);
      throw new HoodieIOException("Failed to create file for write in ExternalSorter", e);
    }
    File fileToWrite = fileToWriteCb.get();
    // write the sorted entries to the file
    try {
      DiskFileWriter diskFileWriter = new DiskFileWriter(fileToWrite, DEFAULT_WRITE_BUFFER);
      for (R r : sortedList) {
        diskFileWriter.write(r);
      }
      diskFileWriter.close();
      success = true;
    } catch (IOException e) {
      LOG.error("Failed to write sorted entries to file", e);
      throw new HoodieIOException("Failed to write sorted entries to file in ExternalSorter", e);
    } finally {
      fileToWriteCb.done(success);
    }
  }

  class DiskFileWriter implements Closeable {
    private final FileOutputStream fileOutputStream;
    private final SizeAwareDataOutputStream writeStream;

    public DiskFileWriter(File file, int writeBufferSize) throws IOException {
      this.fileOutputStream = new FileOutputStream(file, true);
      this.writeStream = new SizeAwareDataOutputStream(fileOutputStream, writeBufferSize);
    }

    public void write(R record) throws IOException {
      // write the record to the file
      Entry entry = Entry.newEntry(serializeFunc.serialize(record));
      entry.writeToFile(writeStream);
    }

    @Override
    public void close() throws IOException {
      writeStream.flush();
      writeStream.close();
    }
  }

  private class SortedFileManager {
    public static final int DEFAULT_FILE_DELETE_RETRY_TIMES = 3;
    private int fileCount;
    private long totalFileSize;
    private List<File> files;

    public SortedFileManager() {
      this.fileCount = 0;
      this.totalFileSize = 0;
      this.files = new LinkedList<>();
    }

    public CompletedCallback<File> createFileForWrite() throws IOException {
      String fileName = String.format("%s/%d.%s", SortOnReadExternalSorter.this.basePath, fileCount, SPILL_FILE_SUFFIX);
      File file = new File(fileName);
      if (!deleteWithRetry(file, DEFAULT_FILE_DELETE_RETRY_TIMES)) {
        throw new HoodieIOException("Failed to delete spill file: " + file.getAbsolutePath());
      }
      if (!file.getParentFile().exists()) {
        file.getParentFile().mkdir();
      }
      file.createNewFile();
      LOG.debug("Created file for write: " + file.getAbsolutePath());
      file.deleteOnExit();
      return CompletedCallback.of(file, success -> {
        if (success) {
          addFile(file);
          return;
        }
        LOG.error("Failed to write file: " + file.getAbsolutePath());
        if (!deleteWithRetry(file, DEFAULT_FILE_DELETE_RETRY_TIMES)) {
          throw new HoodieIOException("Failed to delete spill file: " + file.getAbsolutePath());
        }
      });
    }

    private boolean deleteWithRetry(File file, int retryTimes) {
      for (int i = 0; i < retryTimes && file.exists() && !file.delete(); i++) {
        LOG.warn("Failed to delete spill file: {} for {} times", file.getAbsolutePath(), i + 1);
      }
      return !file.exists();
    }

    public void addFile(File file) {
      this.files.add(file);
      this.fileCount++;
      this.totalFileSize += file.length();
    }

    public long getTotalFileSize() {
      return totalFileSize;
    }

    void clear() {
      files.forEach(File::delete);
      files.clear();
      fileCount = 0;
      totalFileSize = 0;
    }
  }

  private static final class Entry {
    public static final int MAGIC = 0x123321;
    private Integer magic;
    private Long crc;
    private Integer recordSize;
    private byte[] record;

    public Entry(Integer magic, Long crc, Integer recordSize, byte[] record) {
      this.magic = magic;
      this.crc = crc;
      this.recordSize = recordSize;
      this.record = record;
    }

    public Integer getMagic() {
      return magic;
    }

    public Long getCrc() {
      return crc;
    }

    public Integer getRecordSize() {
      return recordSize;
    }

    public byte[] getRecord() {
      return record;
    }

    public static Entry newEntry(byte[] record) {
      return new Entry(MAGIC, BinaryUtil.generateChecksum(record), record.length, record);
    }

    public void writeToFile(SizeAwareDataOutputStream outputStream) throws IOException {
      outputStream.writeInt(magic);
      outputStream.writeLong(crc);
      outputStream.writeInt(recordSize);
      outputStream.write(record);
    }
  }

  private class SortedEntryFileReader extends SortedEntryReader {
    private final File file;
    private final BufferedRandomAccessFile reader;
    private List<R> batchRecords;
    private boolean closed;
    private boolean isEOF;
    private long currentReadBatchSize;

    public SortedEntryFileReader(int readerId, long batchSize, File file) {
      super(readerId, batchSize);
      this.file = file;
      try {
        this.reader = new BufferedRandomAccessFile(file, "r", DEFAULT_READ_BUFFER);
      } catch (IOException e) {
        throw new HoodieIOException("Failed to create BufferedRandomAccessFile", e);
      }
    }

    @Override
    public Iterator<R> readBatch() {
      if (isEOF) {
        return null;
      }
      // TODO: Suitable initial size
      this.batchRecords = new ArrayList<>();
      while (currentReadBatchSize < batchSize) {
        // fetch the next record from the file, and add it to the batch
        Option<Entry> entryOpt = readEntry(reader);
        if (entryOpt.isEmpty()) {
          // reach end
          isEOF = true;
          break;
        }
        Entry entry = entryOpt.get();
        R record = (R) serializeFunc.deserialize(entry.getRecord());
        batchRecords.add(record);
        currentReadBatchSize += sizeEstimator.sizeEstimate(record);
      }
      if (isEOF && currentReadBatchSize == 0) {
        LOG.debug("SortedEntryFileReader: {} reach end", readerId);
        return null;
      }
      LOG.debug("SortedEntryFileReader: {} read batchId: {} with records num: {}, batch total size: {}", readerId, batchId, batchRecords.size(), currentReadBatchSize);
      batchId++;
      return this.batchRecords.iterator();
    }

    private Option<Entry> readEntry(BufferedRandomAccessFile reader) {
      int magic;
      try {
        magic = reader.readInt();
      } catch (IOException e) {
        // reach end
        return Option.empty();
      }
      if (magic != Entry.MAGIC) {
        throw new HoodieIOException("Invalid magic number");
      }
      try {
        long crc = reader.readLong();
        int recordSize = reader.readInt();
        byte[] record = new byte[recordSize];
        reader.readFully(record, 0, recordSize);
        // check crc
        long crcOfReadValue = BinaryUtil.generateChecksum(record);
        if (crc != crcOfReadValue) {
          throw new HoodieIOException("CRC mismatch");
        }
        return Option.of(new Entry(magic, crc, recordSize, record));
      } catch (IOException e) {
        throw new HoodieIOException("Failed to read entry", e);
      }
    }

    @Override
    public void releaseBatch() {
      this.batchRecords.clear();
      this.currentReadBatchSize = 0;
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        batchRecords.clear();
        reader.close();
        closed = true;
      }
    }
  }

  private class SortedEntryMemoryReader extends SortedEntryReader {
    private final List<R> entries;

    public SortedEntryMemoryReader(int readerId, List<R> entries) {
      super(readerId, 0);
      this.entries = entries;
    }

    @Override
    public Iterator<R> readBatch() {
      if (batchId == 0) {
        batchId++;
        return entries.iterator();
      }
      return null;
    }

    @Override
    public void releaseBatch() {
      // no-op
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }

}

