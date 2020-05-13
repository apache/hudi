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

package org.apache.hudi.common.bootstrap.index;

import org.apache.hudi.avro.model.BootstrapIndexInfo;
import org.apache.hudi.avro.model.BootstrapPartitionMetadata;
import org.apache.hudi.avro.model.BootstrapSourceFilePartitionInfo;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BootstrapSourceFileMapping;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Maintains mapping from hudi file id (which contains skeleton file) to external base file.
 * It maintains 2 physical indices.
 *  (a) At partition granularity to lookup all indices for each partition.
 *  (b) At file-group granularity to lookup bootstrap mapping for an individual file-group.
 *
 * This implementation uses HFile as physical storage of index. FOr the initial run, bootstrap
 * mapping for the entire dataset resides in a single file but care has been taken in naming
 * the index files in the same way as Hudi data files so that we can reuse file-system abstraction
 * on these index files to manage multiple file-groups.
 */

public class HFileBasedBootstrapIndex extends BootstrapIndex {

  protected static final long serialVersionUID = 1L;

  private static final Logger LOG = LogManager.getLogger(HFileBasedBootstrapIndex.class);

  public static final String BOOTSTRAP_INDEX_FILE_ID = "00000000-0000-0000-0000-000000000000-0";

  // Used as naming extensions.
  public static final String BOOTSTRAP_INDEX_FILE_TYPE = "hfile";

  // Additional Metadata written to HFiles.
  public static final byte[] INDEX_INFO_KEY = Bytes.toBytes("INDEX_INFO");

  // Flag to idenitfy if Bootstrap Index is empty or not
  private final boolean hasIndex;

  public HFileBasedBootstrapIndex(HoodieTableMetaClient metaClient) {
    super(metaClient);
    Path indexByPartitionPath = getIndexByPartitionPath(metaClient);
    Path indexByFilePath = getIndexByFileIdPath(metaClient);
    try {
      if (metaClient.getFs().exists(indexByPartitionPath) && metaClient.getFs().exists(indexByFilePath)) {
        hasIndex = true;
      } else {
        hasIndex = false;
      }
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  private static String getPartitionKey(String partition) {
    return "part=" + partition;
  }

  private static String getFileGroupKey(HoodieFileGroupId fileGroupId) {
    return "part=" + fileGroupId.getPartitionPath() + ";fileid=" + fileGroupId.getFileId();
  }

  private static Path getIndexByPartitionPath(HoodieTableMetaClient metaClient) {
    return new Path(metaClient.getBootstrapIndexByPartitionFolderName(),
        FSUtils.makeBootstrapIndexFileName(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS, BOOTSTRAP_INDEX_FILE_ID,
            BOOTSTRAP_INDEX_FILE_TYPE));
  }

  private static Path getIndexByFileIdPath(HoodieTableMetaClient metaClient) {
    return new Path(metaClient.getBootstrapIndexByFileIdFolderNameFolderName(),
        FSUtils.makeBootstrapIndexFileName(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS, BOOTSTRAP_INDEX_FILE_ID,
            BOOTSTRAP_INDEX_FILE_TYPE));
  }

  /**
   * Helper method to create HFile Reader.
   *
   * @param hFilePath File Path
   * @param conf Configuration
   * @param fileSystem File System
   */
  private static HFile.Reader createReader(String hFilePath, Configuration conf, FileSystem fileSystem) {
    try {
      LOG.info("Opening HFile for reading :" + hFilePath);
      HFile.Reader reader = HFile.createReader(fileSystem, new HFilePathForReader(hFilePath),
          new CacheConfig(conf), conf);
      return reader;
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  @Override
  public BootstrapIndex.IndexReader createReader() {
    return new HFileBootstrapIndexReader(metaClient);
  }

  @Override
  public BootstrapIndex.IndexWriter createWriter(String sourceBasePath) {
    return new HFileBootstrapIndexWriter(sourceBasePath, metaClient);
  }

  @Override
  public void dropIndex() {
    try {
      Path[] indexPaths = new Path[]{getIndexByPartitionPath(metaClient), getIndexByFileIdPath(metaClient)};
      for (Path indexPath : indexPaths) {
        if (metaClient.getFs().exists(indexPath)) {
          LOG.info("Dropping bootstrap index. Deleting file : " + indexPath);
          metaClient.getFs().delete(indexPath);
        }
      }
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  @Override
  protected boolean checkIndex() {
    return hasIndex;
  }

  /**
   * HFile Based Index Reader.
   */
  public static class HFileBootstrapIndexReader extends BootstrapIndex.IndexReader {

    // Base Path of external files.
    private final String sourceBasePath;
    // Well Known Paths for indices
    private final String indexByPartitionPath;
    private final String indexByFileIdPath;

    // Index Readers
    private transient HFile.Reader indexByPartitionReader;
    private transient HFile.Reader indexByFileIdReader;

    // Bootstrap Index Info
    private transient BootstrapIndexInfo bootstrapIndexInfo;

    public HFileBootstrapIndexReader(HoodieTableMetaClient metaClient) {
      super(metaClient);
      Path indexByPartitionPath = getIndexByPartitionPath(metaClient);
      Path indexByFilePath = getIndexByFileIdPath(metaClient);
      this.indexByPartitionPath = indexByPartitionPath.toString();
      this.indexByFileIdPath = indexByFilePath.toString();
      this.sourceBasePath = getIndexInfo().getSourceBasePath();
      LOG.info("Loaded HFileBasedBootstrapIndex with source base path :" + sourceBasePath);
    }

    @Override
    public BootstrapIndexInfo getIndexInfo() {
      if (null == bootstrapIndexInfo) {
        synchronized (this) {
          if (null == bootstrapIndexInfo) {
            try {
              bootstrapIndexInfo = fetchBootstrapIndexInfo();
            } catch (IOException ioe) {
              throw new HoodieException(ioe.getMessage(), ioe);
            }
          }
        }
      }
      return bootstrapIndexInfo;
    }

    private BootstrapIndexInfo fetchBootstrapIndexInfo() throws IOException {
      return TimelineMetadataUtils.deserializeAvroMetadata(
          getIndexByPartitionReader().loadFileInfo().get(INDEX_INFO_KEY),
          BootstrapIndexInfo.class);
    }

    private HFile.Reader getIndexByPartitionReader() {
      if (null == indexByPartitionReader) {
        synchronized (this) {
          if (null == indexByPartitionReader) {
            LOG.info("Opening partition index :" + indexByPartitionPath);
            this.indexByPartitionReader =
                createReader(indexByPartitionPath, metaClient.getHadoopConf(), metaClient.getFs());
          }
        }
      }
      return indexByPartitionReader;
    }

    private HFile.Reader getIndexByFileIdReader() {
      if (null == indexByFileIdReader) {
        synchronized (this) {
          if (null == indexByFileIdReader) {
            LOG.info("Opening fileId index :" + indexByFileIdPath);
            this.indexByFileIdReader =
                createReader(indexByFileIdPath, metaClient.getHadoopConf(), metaClient.getFs());
          }
        }
      }
      return indexByFileIdReader;
    }

    @Override
    public List<String> getIndexedPartitions() {
      HFileScanner scanner = getIndexByPartitionReader().getScanner(true, true);
      return getAllKeys(scanner);
    }

    @Override
    public List<String> getIndexedFileIds() {
      HFileScanner scanner = getIndexByFileIdReader().getScanner(true, true);
      return getAllKeys(scanner);
    }

    private List<String> getAllKeys(HFileScanner scanner) {
      List<String> keys = new ArrayList<>();
      try {
        boolean available = scanner.seekTo();
        while (available) {
          keys.add(CellUtil.getCellKeyAsString(scanner.getKeyValue()));
          available = scanner.next();
        }
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }

      return keys;
    }

    @Override
    public List<BootstrapSourceFileMapping> getSourceFileMappingForPartition(String partition) {
      try {
        HFileScanner scanner = getIndexByPartitionReader().getScanner(true, true);
        KeyValue keyValue = new KeyValue(Bytes.toBytes(getPartitionKey(partition)), new byte[0], new byte[0],
            HConstants.LATEST_TIMESTAMP, KeyValue.Type.Put, new byte[0]);
        if (scanner.seekTo(keyValue) == 0) {
          ByteBuffer readValue = scanner.getValue();
          byte[] valBytes = Bytes.toBytes(readValue);
          BootstrapPartitionMetadata metadata =
              TimelineMetadataUtils.deserializeAvroMetadata(valBytes, BootstrapPartitionMetadata.class);
          return metadata.getHudiFileIdToSourceFile().entrySet().stream()
              .map(e -> new BootstrapSourceFileMapping(sourceBasePath, metadata.getSourcePartitionPath(),
                  partition, e.getValue(), e.getKey())).collect(Collectors.toList());
        } else {
          LOG.info("No value found for partition key (" + partition + ")");
          return new ArrayList<>();
        }
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    }

    @Override
    public String getSourceBasePath() {
      return sourceBasePath;
    }

    @Override
    public Map<HoodieFileGroupId, BootstrapSourceFileMapping> getSourceFileMappingForFileIds(
        List<HoodieFileGroupId> ids) {
      Map<HoodieFileGroupId, BootstrapSourceFileMapping> result = new HashMap<>();
      // Arrange input Keys in sorted order for 1 pass scan
      List<HoodieFileGroupId> fileGroupIds = new ArrayList<>(ids);
      Collections.sort(fileGroupIds);
      try {
        HFileScanner scanner = getIndexByFileIdReader().getScanner(true, true);
        for (HoodieFileGroupId fileGroupId : fileGroupIds) {
          KeyValue keyValue = new KeyValue(Bytes.toBytes(getFileGroupKey(fileGroupId)), new byte[0], new byte[0],
              HConstants.LATEST_TIMESTAMP, KeyValue.Type.Put, new byte[0]);
          if (scanner.seekTo(keyValue) == 0) {
            ByteBuffer readValue = scanner.getValue();
            byte[] valBytes = Bytes.toBytes(readValue);
            BootstrapSourceFilePartitionInfo fileInfo = TimelineMetadataUtils.deserializeAvroMetadata(valBytes,
                BootstrapSourceFilePartitionInfo.class);
            BootstrapSourceFileMapping mapping = new BootstrapSourceFileMapping(sourceBasePath,
                fileInfo.getSourcePartitionPath(), fileInfo.getHudiPartitionPath(), fileInfo.getSourceFileStatus(),
                fileGroupId.getFileId());
            result.put(fileGroupId, mapping);
          }
        }
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
      return result;
    }

    @Override
    public void close() {
      try {
        if (indexByPartitionReader != null) {
          indexByPartitionReader.close(true);
          indexByPartitionReader = null;
        }
        if (indexByFileIdReader != null) {
          indexByFileIdReader.close(true);
          indexByFileIdReader = null;
        }
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    }
  }

  /**
   * Boostrap Index Writer to build bootstrap index.
   */
  public static class HFileBootstrapIndexWriter extends BootstrapIndex.IndexWriter {

    private final String sourceBasePath;
    private final Path indexByPartitionPath;
    private final Path indexByFileIdPath;
    private HFile.Writer indexByPartitionWriter;
    private HFile.Writer indexByFileIdWriter;

    private boolean closed = false;
    private int numPartitionKeysAdded = 0;
    private int numFileIdKeysAdded = 0;

    private final Map<String, List<BootstrapSourceFileMapping>> sourceFileMappings = new HashMap<>();

    private HFileBootstrapIndexWriter(String sourceBasePath, HoodieTableMetaClient metaClient) {
      super(metaClient);
      try {
        metaClient.initializeBootstrapDirsIfNotExists();
        this.sourceBasePath = sourceBasePath;
        this.indexByPartitionPath = getIndexByPartitionPath(metaClient);
        this.indexByFileIdPath = getIndexByFileIdPath(metaClient);

        if (metaClient.getFs().exists(indexByPartitionPath) || metaClient.getFs().exists(indexByFileIdPath)) {
          String errMsg = "Previous version of bootstrap index exists. Partition Index Path :" + indexByPartitionPath
              + ", FileId index Path :" + indexByFileIdPath;
          LOG.info(errMsg);
          throw new HoodieException(errMsg);
        }
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    }

    /**
     * Append bootstrap index entries for next partitions in sorted order.
     * @param hudiPartitionPath    Hudi Partition Path
     * @param sourcePartitionPath  Source Partition Path
     * @param bootstrapSourceFileMappings   Bootstrap Source File to Hudi File Id mapping
     */
    private void writeNextPartition(String hudiPartitionPath, String sourcePartitionPath,
        List<BootstrapSourceFileMapping> bootstrapSourceFileMappings) {
      try {
        LOG.info("Adding bootstrap partition Index entry for partition :" + hudiPartitionPath
            + ", Source Partition :" + sourcePartitionPath + ", Num Entries :" + bootstrapSourceFileMappings.size());
        LOG.info("ADDING entries :" + bootstrapSourceFileMappings);
        BootstrapPartitionMetadata bootstrapPartitionMetadata = new BootstrapPartitionMetadata();
        bootstrapPartitionMetadata.setSourcePartitionPath(sourcePartitionPath);
        bootstrapPartitionMetadata.setHudiPartitionPath(hudiPartitionPath);
        bootstrapPartitionMetadata.setHudiFileIdToSourceFile(
            bootstrapSourceFileMappings.stream().map(m -> Pair.of(m.getHudiFileId(),
                m.getSourceFileStatus())).collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
        Option<byte[]> bytes = TimelineMetadataUtils.serializeAvroMetadata(bootstrapPartitionMetadata,
            BootstrapPartitionMetadata.class);
        if (bytes.isPresent()) {
          indexByPartitionWriter
              .append(new KeyValue(Bytes.toBytes(getPartitionKey(hudiPartitionPath)), new byte[0], new byte[0],
                  HConstants.LATEST_TIMESTAMP, KeyValue.Type.Put, bytes.get()));
          numPartitionKeysAdded++;
        }
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    }

    /**
     * Write next source file to hudi file-id. Entries are expected to be appended in hudi file-group id
     * order.
     * @param mapping boostrap source file mapping.
     */
    private void writeNextSourceFileMapping(BootstrapSourceFileMapping mapping) {
      try {
        BootstrapSourceFilePartitionInfo srcFilePartitionInfo = new BootstrapSourceFilePartitionInfo();
        srcFilePartitionInfo.setHudiPartitionPath(mapping.getHudiPartitionPath());
        srcFilePartitionInfo.setSourcePartitionPath(mapping.getSourcePartitionPath());
        srcFilePartitionInfo.setSourceFileStatus(mapping.getSourceFileStatus());
        KeyValue kv = new KeyValue(getFileGroupKey(mapping.getFileGroupId()).getBytes(), new byte[0], new byte[0],
            HConstants.LATEST_TIMESTAMP, KeyValue.Type.Put,
            TimelineMetadataUtils.serializeAvroMetadata(srcFilePartitionInfo,
                BootstrapSourceFilePartitionInfo.class).get());
        indexByFileIdWriter.append(kv);
        numFileIdKeysAdded++;
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    }

    /**
     * Commit bootstrap index entries. Appends Metadata and closes write handles.
     */
    private void commit() {
      try {
        if (!closed) {
          BootstrapIndexInfo partitionIndexInfo = BootstrapIndexInfo.newBuilder()
              .setCreatedTimestamp(new Date().getTime())
              .setNumKeys(numPartitionKeysAdded)
              .setSourceBasePath(sourceBasePath)
              .build();
          LOG.info("Adding Partition FileInfo :" + partitionIndexInfo);

          BootstrapIndexInfo fileIdIndexInfo = BootstrapIndexInfo.newBuilder()
              .setCreatedTimestamp(new Date().getTime())
              .setNumKeys(numFileIdKeysAdded)
              .setSourceBasePath(sourceBasePath)
              .build();
          LOG.info("Appending FileId FileInfo :" + fileIdIndexInfo);

          indexByPartitionWriter.appendFileInfo(INDEX_INFO_KEY,
              TimelineMetadataUtils.serializeAvroMetadata(partitionIndexInfo, BootstrapIndexInfo.class).get());
          indexByFileIdWriter.appendFileInfo(INDEX_INFO_KEY,
              TimelineMetadataUtils.serializeAvroMetadata(fileIdIndexInfo, BootstrapIndexInfo.class).get());

          close();
        }
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    }

    /**
     * Close Writer Handles.
     */
    public void close() {
      try {
        if (!closed) {
          indexByPartitionWriter.close();
          indexByFileIdWriter.close();
          closed = true;
        }
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    }

    @Override
    public void begin() {
      try {
        HFileContext meta = new HFileContextBuilder().build();
        this.indexByPartitionWriter = HFile.getWriterFactory(metaClient.getHadoopConf(),
            new CacheConfig(metaClient.getHadoopConf())).withPath(metaClient.getFs(), indexByPartitionPath)
            .withFileContext(meta).withComparator(new KeyValue.KVComparator()).create();
        this.indexByFileIdWriter = HFile.getWriterFactory(metaClient.getHadoopConf(),
            new CacheConfig(metaClient.getHadoopConf())).withPath(metaClient.getFs(), indexByFileIdPath)
            .withFileContext(meta).withComparator(new KeyValue.KVComparator()).create();
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    }

    @Override
    public void appendNextPartition(String hudiPartitionPath, List<BootstrapSourceFileMapping> bootstrapSourceFileMappings) {
      sourceFileMappings.put(hudiPartitionPath, bootstrapSourceFileMappings);
    }

    @Override
    public void finish() {
      // Sort and write
      List<String> partitions = sourceFileMappings.keySet().stream().sorted().collect(Collectors.toList());
      partitions.forEach(p -> writeNextPartition(p, sourceFileMappings.get(p).get(0).getSourcePartitionPath(),
          sourceFileMappings.get(p)));
      sourceFileMappings.values().stream().flatMap(Collection::stream).sorted()
          .forEach(this::writeNextSourceFileMapping);
      commit();
    }
  }

  /**
   * IMPORTANT :
   * HFile Readers use HFile name (instead of path) as cache key. This could be fine as long
   * as file names are UUIDs. For bootstrap, we are using well-known index names.
   * Hence, this hacky workaround to return full path string from Path subclass and pass it to reader.
   * The other option is to disable block cache for Bootstrap which again involves some custom code
   * as there is no API to disable cache.
   */
  private static class HFilePathForReader extends Path {

    public HFilePathForReader(String pathString) throws IllegalArgumentException {
      super(pathString);
    }

    @Override
    public String getName() {
      return toString();
    }
  }
}
