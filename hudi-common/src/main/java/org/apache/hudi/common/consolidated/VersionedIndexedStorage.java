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

package org.apache.hudi.common.consolidated;

import org.apache.hudi.common.SerializableConfiguration;
import org.apache.hudi.common.util.ConsistencyGuardConfig;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class VersionedIndexedStorage implements Serializable {

  private static Logger log = LogManager.getLogger(VersionedIndexedStorage.class);

  private final String namespace;
  private final SerializableConfiguration conf;
  private final ConsistencyGuardConfig consistencyGuardConfig;
  private final String rootMetadataDir;
  private final String rootMetadataIndexDir;

  private transient FileSystem fileSystem;

  public VersionedIndexedStorage(String namespace, SerializableConfiguration conf,
      ConsistencyGuardConfig consistencyGuardConfig, String rootDir, String rootIndexDir) {
    this.namespace = namespace;
    this.rootMetadataDir = rootDir;
    this.rootMetadataIndexDir = rootIndexDir;
    this.consistencyGuardConfig = consistencyGuardConfig;
    this.conf = new SerializableConfiguration(conf);
    this.fileSystem = getFileSystem();
  }

  private FileSystem getFileSystem() {
    if (null == fileSystem) {
      fileSystem = FSUtils.getFs(rootMetadataDir, conf, consistencyGuardConfig);
    }
    return fileSystem;
  }

  public DataPayloadWriter getDataPayloadWriter(String currVersion, long maxFileSizeInBytes) {
    return new DataPayloadWriter(maxFileSizeInBytes, currVersion);
  }

  public IndexWriter getIndexWriter(String prevVersion, String currVersion) {
    return new IndexWriter(prevVersion, currVersion);
  }

  public RandomAccessReader getRandomAccessReader(String version) throws IOException {
    return new RandomAccessReader(version);
  }

  public boolean isIndexExists(String version) {
    Path p = getIndexFilePath(version);
    try {
      return getFileSystem().isFile(p);
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  protected Path getNextOutputFilePath(String currVersion, int id) {
    return new Path(rootMetadataDir, namespace + "_" + currVersion + "_" + id + ".seq");
  }

  protected Path getIndexFilePath(String version) {
    return new Path(rootMetadataIndexDir, "keyidx_" + namespace + "_" + version + ".seq");
  }

  /**
   * Random Access Reader
   */
  public class RandomAccessReader {

    private final String version;

    private Map<String, Pair<String, Long>> keyToFileAndOffsetPair = new HashMap<>();

    public RandomAccessReader(String version) throws IOException {
      this.version = version;
      keyToFileAndOffsetPair = readExistingKeyToFileOffsetIndex(new SequenceFile.Reader(conf.get(),
          SequenceFile.Reader.file(getIndexFilePath(this.version))));
    }

    public int size() {
      return keyToFileAndOffsetPair.size();
    }

    public boolean isEmpty() {
      return keyToFileAndOffsetPair.isEmpty();
    }

    public boolean containsKey(Object key) {
      return keyToFileAndOffsetPair.containsKey(key);
    }

    public byte[] get(Object key) {
      Pair<String, Long> val = keyToFileAndOffsetPair.get(key);
      if (null != val) {
        try (SequenceFile.Reader reader = new SequenceFile.Reader(conf.get(),
            SequenceFile.Reader.file(new Path(rootMetadataDir, val.getKey())))) {
          reader.seek(val.getValue());
          Text keyText = new Text();
          BytesWritable valWritable = new BytesWritable();
          reader.next(keyText, valWritable);
          Preconditions.checkArgument(keyText.toString().equals(key), "Expected=" + key + ", Got="
              + keyText.toString());
          return valWritable.getBytes();
        } catch (IOException ioe) {
          log.error("Got exception reading data from " + val);
          throw new HoodieIOException(ioe.getMessage(), ioe);
        }
      }
      return null;
    }
  }

  /**
   * Responsible for writing index to payload
   */
  public class IndexWriter {

    private SequenceFile.Reader prevConsolidatedIdxReader;
    private final String currVersion;
    private final String prevVersion;
    private Map<String, Pair<String, Long>> mergedKeyToFileOffsetIdx;
    private FSDataOutputStream outputStream;
    private SequenceFile.Writer idxWriter;

    public IndexWriter(String prevVersion, String currVersion) {
      this.currVersion = currVersion;
      this.prevVersion = prevVersion;
      init();
    }

    private void init() {
      try {
        if (null != prevVersion) {
          prevConsolidatedIdxReader = new SequenceFile.Reader(conf.get(),
              SequenceFile.Reader.file(getIndexFilePath(prevVersion)));
          mergedKeyToFileOffsetIdx = readExistingKeyToFileOffsetIndex(prevConsolidatedIdxReader);
        } else {
          prevConsolidatedIdxReader = null;
          mergedKeyToFileOffsetIdx = new HashMap<>();
        }
        log.info("Read " + mergedKeyToFileOffsetIdx.size() + " from previous index");
        outputStream = getFileSystem().create(getIndexFilePath(currVersion), false);
        idxWriter = SequenceFile.createWriter(conf.get(),
            SequenceFile.Writer.keyClass(Text.class),
            SequenceFile.Writer.valueClass(Text.class),
            SequenceFile.Writer.compression(CompressionType.NONE),
            SequenceFile.Writer.stream(outputStream));
      } catch (IOException ioe) {
        log.error("Got exception starting the writer", ioe);
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    }

    public void addIndexEntries(Stream<Pair<String, Pair<Integer, Long>>> partitionToFileOffsetEntries)
        throws IOException {
      partitionToFileOffsetEntries.forEach(e -> {
        String key = e.getKey();
        String fileName = getNextOutputFilePath(currVersion, e.getValue().getKey()).getName();
        Long offset = e.getValue().getValue();
        mergedKeyToFileOffsetIdx.put(key, Pair.of(fileName, offset));
      });
    }

    public void rollback() throws IOException {
      close();
    }

    public void commit() throws IOException {
      try {
        for (Entry<String, Pair<String, Long>> entry : mergedKeyToFileOffsetIdx.entrySet()) {
          Text key = new Text(entry.getKey());
          Text value = new Text(entry.getValue().getValue() + "," + entry.getValue().getKey());
          idxWriter.append(key, value);
        }
      } catch (IOException ioe) {
        log.error("Got exception trying to save the file offset index");
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }

      // Nothing to be done
      close();
    }

    private void close() throws IOException {
      if (prevConsolidatedIdxReader != null) {
        prevConsolidatedIdxReader.close();
        prevConsolidatedIdxReader = null;
      }

      if (null != outputStream) {
        outputStream.close();
      }

      if (null != idxWriter) {
        idxWriter.close();
      }

      mergedKeyToFileOffsetIdx = null;
    }
  }

  /**
   * Responsible for Writing payload
   */
  public class DataPayloadWriter {

    private final long maxFileSizeInBytes;
    private final String currVersion;

    private SequenceFile.Writer currWriter;
    private FSDataOutputStream currOuputStream;
    private Path currOutputFilePath;
    private int idGen;
    private Map<String, Pair<Integer, Long>> keyToFileAndOffsetPair = new HashMap<>();

    public DataPayloadWriter(long maxFileSizeInBytes, String currVersion) {
      this.maxFileSizeInBytes = maxFileSizeInBytes;
      this.currVersion = currVersion;
      init();
    }

    private void init() {
      try {
        this.idGen = -1;
        rollover();
      } catch (IOException ioe) {
        log.error("Got exception starting the writer", ioe);
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    }

    private void rollover() throws IOException {
      if (currOuputStream != null) {
        currOuputStream.close();
      }

      if (currWriter != null) {
        currWriter.close();
      }

      this.currOutputFilePath = getNextOutputFilePathForCurrentVersion();
      this.currOuputStream = getFileSystem().create(currOutputFilePath, false);
      this.currWriter = SequenceFile.createWriter(conf.get(),
          SequenceFile.Writer.keyClass(Text.class),
          SequenceFile.Writer.valueClass(BytesWritable.class),
          SequenceFile.Writer.compression(CompressionType.RECORD),
          SequenceFile.Writer.stream(currOuputStream));
    }

    private Path getNextOutputFilePathForCurrentVersion() {
      int currId = ++idGen;
      return getNextOutputFilePath(currVersion, currId);
    }

    public void update(Stream<Pair<String, byte[]>> entries) throws IOException {
      entries.forEach(entry -> {
        try {
          update(entry.getKey(), entry.getRight());
        } catch (IOException e) {
          throw new HoodieIOException(e.getMessage(), e);
        }
      });
    }

    public void update(String keyStr, byte[] payload) throws IOException {
      Text key = new Text(keyStr);
      BytesWritable bytesWritable = new BytesWritable(payload);
      try {
        keyToFileAndOffsetPair.put(keyStr, Pair.of(idGen, currWriter.getLength()));
        this.currWriter.append(key, bytesWritable);
        if (currWriter.getLength() >= maxFileSizeInBytes) {
          rollover();
        }
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    }

    public List<Pair<String, Pair<Integer, Long>>> getWrittenFileNameOffsets() {
      return keyToFileAndOffsetPair.entrySet().stream().map(entry -> Pair.of(entry.getKey(), entry.getValue())).collect(
          Collectors.toList());
    }

    public void close() throws IOException {
      if (currOuputStream != null) {
        currOuputStream.close();
        currOuputStream = null;
      }

      if (currWriter != null) {
        currWriter.close();
        currWriter = null;
      }

      keyToFileAndOffsetPair = null;
    }
  }

  private static Map<String, Pair<String, Long>> readExistingKeyToFileOffsetIndex(
      SequenceFile.Reader prevConsolidatedIdxReader) throws IOException {
    Map<String, Pair<String, Long>> existingKeyToFileOffsetIdx = new HashMap<>();
    if (prevConsolidatedIdxReader != null) {
      // Consolidated Metada already existed. We need to merge only the file-offsets
      Text key = new Text();
      Text offsetFileCsv = new Text();
      while (prevConsolidatedIdxReader.next(key, offsetFileCsv)) {
        String[] offsetFileNames = offsetFileCsv.toString().split(",");
        Preconditions.checkArgument(offsetFileNames.length == 2,
            "offsetFileCsv not is valid format. offsetFileCsv=" + offsetFileCsv);
        Long offset = Long.parseLong(offsetFileNames[0]);
        String fileName = offsetFileNames[1];
        existingKeyToFileOffsetIdx.put(key.toString(), Pair.of(fileName, offset));
      }
    }
    return existingKeyToFileOffsetIdx;
  }
}
