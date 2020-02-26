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

package org.apache.hudi.common.bootstrap;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
import org.apache.hudi.avro.model.BootstrapIndexInfo;
import org.apache.hudi.avro.model.BootstrapPartitionMetadata;
import org.apache.hudi.avro.model.BootstrapSourceFilePartitionInfo;
import org.apache.hudi.avro.model.SourceFileInfo;
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

public class BootstrapIndex implements Serializable, AutoCloseable {

  private static final Logger LOG = LogManager.getLogger(BootstrapIndex.class);

  public static final String BOOTSTRAP_INDEX_FILE_ID = "00000000-0000-0000-0000-000000000000-0";

  // Used as naming extensions.
  public static final String BOOTSTRAP_INDEX_FILE_TYPE = "hfile";

  // Additional Metadata written to HFiles.
  public static final byte[] INDEX_INFO_KEY = Bytes.toBytes("INDEX_INFO");

  private final HoodieTableMetaClient metaClient;
  // Base Path of external files.
  private final String sourceBasePath;
  // Well Known Paths for indices
  private final String indexByPartitionPath;
  private final String indexByFileIdPath;
  // Flag to idenitfy if Bootstrap Index is empty or not
  private final boolean isBootstrapped;

  // Index Readers
  private transient HFile.Reader indexByPartitionReader;
  private transient HFile.Reader indexByFileIdReader;

  // Bootstrap Index Info
  private transient BootstrapIndexInfo bootstrapIndexInfo;

  public BootstrapIndex(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;

    try {
      metaClient.initializeBootstrapDirsIfNotExists();
      Path indexByPartitionPath = getIndexByPartitionPath(metaClient);
      Path indexByFilePath = getIndexByFileIdPath(metaClient);
      if (metaClient.getFs().exists(indexByPartitionPath) && metaClient.getFs().exists(indexByFilePath)) {
        this.indexByPartitionPath = indexByPartitionPath.toString();
        this.indexByFileIdPath = indexByFilePath.toString();
        this.sourceBasePath = getBootstrapIndexInfo().getSourceBasePath();
        this.isBootstrapped = true;
      } else {
        this.indexByPartitionPath = null;
        this.indexByFileIdPath = null;
        this.sourceBasePath = null;
        this.isBootstrapped = false;
      }
      LOG.info("Loaded BootstrapIndex with source base path :" + sourceBasePath);
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  public BootstrapIndexInfo getBootstrapIndexInfo() {
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

  private static Path getIndexByPartitionPath(HoodieTableMetaClient metaClient) {
    return new Path(metaClient.getBootstrapIndexByPartitionFolderName(),
        FSUtils.makeBootstrapIndexFileName(HoodieTimeline.BOOTSTRAP_INSTANT_TS, BOOTSTRAP_INDEX_FILE_ID,
            BOOTSTRAP_INDEX_FILE_TYPE));
  }

  private static Path getIndexByFileIdPath(HoodieTableMetaClient metaClient) {
    return new Path(metaClient.getBootstrapIndexByFileIdFolderNameFolderName(),
        FSUtils.makeBootstrapIndexFileName(HoodieTimeline.BOOTSTRAP_INSTANT_TS, BOOTSTRAP_INDEX_FILE_ID,
            BOOTSTRAP_INDEX_FILE_TYPE));
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

  public List<String> getAllBootstrapPartitionKeys() {
    HFileScanner scanner = getIndexByPartitionReader().getScanner(true, true);
    return getAllKeys(scanner);
  }

  public List<String> getAllBootstrapFileIdKeys() {
    HFileScanner scanner = getIndexByFileIdReader().getScanner(true, true);
    return getAllKeys(scanner);
  }

  private List<String> getAllKeys(HFileScanner scanner) {
    List<String> keys = new ArrayList<>();
    if (isBootstrapped) {
      try {
        boolean available = scanner.seekTo();
        while (available) {
          keys.add(CellUtil.getCellKeyAsString(scanner.getKeyValue()));
          available = scanner.next();
        }
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    }
    return keys;
  }

  /**
   * Drop Bootstrap Index.
   */
  public void dropIndex() {
    try {
      Path[] indexPaths = new Path[] { new Path(indexByPartitionPath), new Path(indexByFileIdPath) };
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

  /**
   * Lookup bootstrap index by partition.
   * @param partition Partition to lookup
   * @return
   */
  public List<BootstrapSourceFileMapping> getBootstrapInfoForPartition(String partition) {
    if (isBootstrapped) {
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
              .map(e -> new BootstrapSourceFileMapping(sourceBasePath, metadata.getSourcePartitionPath(), partition,
                  e.getValue().getFileName(), e.getKey())).collect(Collectors.toList());
        } else {
          LOG.info("No value found for partition key (" + partition + ")");
          return new ArrayList<>();
        }
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    }
    return new ArrayList<>();
  }

  /**
   * Lookup Bootstrap index by file group ids.
   * @param ids File Group Ids
   * @return
   */
  public Map<HoodieFileGroupId, BootstrapSourceFileMapping> getBootstrapInfoForFileIds(List<HoodieFileGroupId> ids) {
    Map<HoodieFileGroupId, BootstrapSourceFileMapping> result = new HashMap<>();
    // Arrange input Keys in sorted order for 1 pass scan
    List<HoodieFileGroupId> fileGroupIds = new ArrayList<>(ids);
    Collections.sort(fileGroupIds);
    if (isBootstrapped) {
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
                fileInfo.getSourcePartitionPath(), fileInfo.getHudiPartitionPath(), fileInfo.getSourceFileName(),
                fileGroupId.getFileId());
            result.put(fileGroupId, mapping);
          }
        }
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    }
    return result;
  }

  /**
   * Helper method to create HFile Reader.
   * @param hFilePath File Path
   * @param conf  Configuration
   * @param fileSystem File System
   * @return
   */
  private static HFile.Reader createReader(String hFilePath, Configuration conf, FileSystem fileSystem) {
    try {
      LOG.info("Opening HFile for reading :" + hFilePath);
      CacheConfig config = new CacheConfig(conf);
      HFile.Reader reader = HFile.createReader(fileSystem, new HFilePathForReader(hFilePath), new CacheConfig(conf),
          conf);
      return reader;
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  public static BootstrapIndexWriter writer(String sourceBasePath, HoodieTableMetaClient metaClient)
      throws IOException {
    return new BootstrapIndexWriter(sourceBasePath, metaClient);
  }

  public static String getPartitionKey(String partition) {
    return "part=" + partition;
  }

  public static String getFileGroupKey(HoodieFileGroupId fileGroupId) {
    return "part=" + fileGroupId.getPartitionPath() + ";fileid=" + fileGroupId.getFileId();
  }

  @Override
  public void close() {
    try {
      indexByPartitionReader.close(true);
      indexByFileIdReader.close(true);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * Boostrap Index Writer to build bootstrap index.
   */
  public static class BootstrapIndexWriter implements AutoCloseable {

    private final String sourceBasePath;
    private final Path indexByPartitionPath;
    private final Path indexByFileIdPath;
    private final HoodieTableMetaClient metaClient;
    private final HFile.Writer indexByPartitionWriter;
    private final HFile.Writer indexByFileIdWriter;

    private boolean closed = false;
    private int numPartitionKeysAdded = 0;
    private int numFileIdKeysAdded = 0;

    private BootstrapIndexWriter(String sourceBasePath, HoodieTableMetaClient metaClient)
        throws IOException {
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

      this.metaClient = metaClient;
      HFileContext meta = new HFileContextBuilder().build();
      this.indexByPartitionWriter = HFile.getWriterFactory(metaClient.getHadoopConf(),
          new CacheConfig(metaClient.getHadoopConf())).withPath(metaClient.getFs(), indexByPartitionPath)
          .withFileContext(meta).withComparator(new KeyValue.KVComparator()).create();
      this.indexByFileIdWriter = HFile.getWriterFactory(metaClient.getHadoopConf(),
                    new CacheConfig(metaClient.getHadoopConf())).withPath(metaClient.getFs(), indexByFileIdPath)
                    .withFileContext(meta).withComparator(new KeyValue.KVComparator()).create();
    }

    /**
     * Append bootstrap index entries for next partitions in sorted order.
     * @param sourcePartitionPath  Source Partition Path
     * @param hudiPartitionPath    Hudi Partition Path
     * @param bootstrapSourceFileMappings   Bootstrap Source File to Hudi File Id mapping
     */
    public void appendNextPartition(String sourcePartitionPath, String hudiPartitionPath,
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
                new SourceFileInfo(m.getSourceFileName()))).collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
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
     * Appends next source file to hudi file-id. Entries are expected to be appended in hudi file-group id
     * order.
     * @param mapping boostrap source file mapping.
     */
    public void appendNextSourceFileMapping(BootstrapSourceFileMapping mapping) {
      try {
        BootstrapSourceFilePartitionInfo srcFilePartitionInfo = new BootstrapSourceFilePartitionInfo();
        srcFilePartitionInfo.setHudiPartitionPath(mapping.getHudiPartitionPath());
        srcFilePartitionInfo.setSourcePartitionPath(mapping.getSourcePartitionPath());
        srcFilePartitionInfo.setSourceFileName(mapping.getSourceFileName());
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
    public void commit() {
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
