/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.bootstrap.index.hfile;

import org.apache.hudi.avro.model.HoodieBootstrapFilePartitionInfo;
import org.apache.hudi.avro.model.HoodieBootstrapIndexInfo;
import org.apache.hudi.avro.model.HoodieBootstrapPartitionMetadata;
import org.apache.hudi.common.bootstrap.index.BootstrapIndex;
import org.apache.hudi.common.model.BootstrapFileMapping;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.hadoop.HoodieHFileUtils;
import org.apache.hudi.io.util.IOUtils;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex.HFILE_CELL_KEY_SUFFIX_PART;
import static org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex.INDEX_INFO_KEY;
import static org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex.fileIdIndexPath;
import static org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex.getFileGroupKey;
import static org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex.getPartitionKey;
import static org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex.partitionIndexPath;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * HBase HFile reader based Index Reader.  This is deprecated.
 */
public class HBaseHFileBootstrapIndexReader extends BootstrapIndex.IndexReader {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseHFileBootstrapIndexReader.class);

  // Base Path of external files.
  private final String bootstrapBasePath;
  // Well Known Paths for indices
  private final String indexByPartitionPath;
  private final String indexByFileIdPath;

  // Index Readers
  private transient HFile.Reader indexByPartitionReader;
  private transient HFile.Reader indexByFileIdReader;

  // Bootstrap Index Info
  private transient HoodieBootstrapIndexInfo bootstrapIndexInfo;

  public HBaseHFileBootstrapIndexReader(HoodieTableMetaClient metaClient) {
    super(metaClient);
    StoragePath indexByPartitionPath = partitionIndexPath(metaClient);
    StoragePath indexByFilePath = fileIdIndexPath(metaClient);
    this.indexByPartitionPath = indexByPartitionPath.toString();
    this.indexByFileIdPath = indexByFilePath.toString();
    initIndexInfo();
    this.bootstrapBasePath = bootstrapIndexInfo.getBootstrapBasePath();
    LOG.info("Loaded HFileBasedBootstrapIndex with source base path :" + bootstrapBasePath);
  }

  /**
   * HFile stores cell key in the format example : "2020/03/18//LATEST_TIMESTAMP/Put/vlen=3692/seqid=0".
   * This API returns only the user key part from it.
   *
   * @param cellKey HFIle Cell Key
   * @return
   */
  private static String getUserKeyFromCellKey(String cellKey) {
    int hfileSuffixBeginIndex = cellKey.lastIndexOf(HFILE_CELL_KEY_SUFFIX_PART);
    return cellKey.substring(0, hfileSuffixBeginIndex);
  }

  /**
   * Helper method to create HFile Reader.
   *
   * @param hFilePath  File Path
   * @param conf       Configuration
   * @param fileSystem File System
   */
  private static HFile.Reader createReader(String hFilePath, Configuration conf, FileSystem fileSystem) {
    return HoodieHFileUtils.createHFileReader(fileSystem, new HFilePathForReader(hFilePath), new CacheConfig(conf), conf);
  }

  private void initIndexInfo() {
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

  private HoodieBootstrapIndexInfo fetchBootstrapIndexInfo() throws IOException {
    return TimelineMetadataUtils.deserializeAvroMetadata(
        partitionIndexReader().getHFileInfo().get(INDEX_INFO_KEY),
        HoodieBootstrapIndexInfo.class);
  }

  private HFile.Reader partitionIndexReader() {
    if (null == indexByPartitionReader) {
      synchronized (this) {
        if (null == indexByPartitionReader) {
          LOG.info("Opening partition index :" + indexByPartitionPath);
          this.indexByPartitionReader = createReader(
              indexByPartitionPath, metaClient.getStorageConf().unwrapAs(Configuration.class), (FileSystem) metaClient.getStorage().getFileSystem());
        }
      }
    }
    return indexByPartitionReader;
  }

  private HFile.Reader fileIdIndexReader() {
    if (null == indexByFileIdReader) {
      synchronized (this) {
        if (null == indexByFileIdReader) {
          LOG.info("Opening fileId index :" + indexByFileIdPath);
          this.indexByFileIdReader = createReader(
              indexByFileIdPath, metaClient.getStorageConf().unwrapAs(Configuration.class), (FileSystem) metaClient.getStorage().getFileSystem());
        }
      }
    }
    return indexByFileIdReader;
  }

  @Override
  public List<String> getIndexedPartitionPaths() {
    try (HFileScanner scanner = partitionIndexReader().getScanner(true, false)) {
      return getAllKeys(scanner, HFileBootstrapIndex::getPartitionFromKey);
    }
  }

  @Override
  public List<HoodieFileGroupId> getIndexedFileGroupIds() {
    try (HFileScanner scanner = fileIdIndexReader().getScanner(true, false)) {
      return getAllKeys(scanner, HFileBootstrapIndex::getFileGroupFromKey);
    }
  }

  private <T> List<T> getAllKeys(HFileScanner scanner, Function<String, T> converter) {
    List<T> keys = new ArrayList<>();
    try {
      boolean available = scanner.seekTo();
      while (available) {
        keys.add(converter.apply(getUserKeyFromCellKey(CellUtil.getCellKeyAsString(scanner.getCell()))));
        available = scanner.next();
      }
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }

    return keys;
  }

  @Override
  public List<BootstrapFileMapping> getSourceFileMappingForPartition(String partition) {
    try (HFileScanner scanner = partitionIndexReader().getScanner(true, false)) {
      KeyValue keyValue = new KeyValue(getUTF8Bytes(getPartitionKey(partition)), new byte[0], new byte[0],
          HConstants.LATEST_TIMESTAMP, KeyValue.Type.Put, new byte[0]);
      if (scanner.seekTo(keyValue) == 0) {
        ByteBuffer readValue = scanner.getValue();
        byte[] valBytes = IOUtils.toBytes(readValue);
        HoodieBootstrapPartitionMetadata metadata =
            TimelineMetadataUtils.deserializeAvroMetadata(valBytes, HoodieBootstrapPartitionMetadata.class);
        return metadata.getFileIdToBootstrapFile().entrySet().stream()
            .map(e -> new BootstrapFileMapping(bootstrapBasePath, metadata.getBootstrapPartitionPath(),
                partition, e.getValue(), e.getKey())).collect(Collectors.toList());
      } else {
        LOG.warn("No value found for partition key (" + partition + ")");
        return new ArrayList<>();
      }
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  @Override
  public String getBootstrapBasePath() {
    return bootstrapBasePath;
  }

  @Override
  public Map<HoodieFileGroupId, BootstrapFileMapping> getSourceFileMappingForFileIds(
      List<HoodieFileGroupId> ids) {
    Map<HoodieFileGroupId, BootstrapFileMapping> result = new HashMap<>();
    // Arrange input Keys in sorted order for 1 pass scan
    List<HoodieFileGroupId> fileGroupIds = new ArrayList<>(ids);
    Collections.sort(fileGroupIds);
    try (HFileScanner scanner = fileIdIndexReader().getScanner(true, false)) {
      for (HoodieFileGroupId fileGroupId : fileGroupIds) {
        KeyValue keyValue = new KeyValue(getUTF8Bytes(getFileGroupKey(fileGroupId)), new byte[0], new byte[0],
            HConstants.LATEST_TIMESTAMP, KeyValue.Type.Put, new byte[0]);
        if (scanner.seekTo(keyValue) == 0) {
          ByteBuffer readValue = scanner.getValue();
          byte[] valBytes = IOUtils.toBytes(readValue);
          HoodieBootstrapFilePartitionInfo fileInfo = TimelineMetadataUtils.deserializeAvroMetadata(valBytes,
              HoodieBootstrapFilePartitionInfo.class);
          BootstrapFileMapping mapping = new BootstrapFileMapping(bootstrapBasePath,
              fileInfo.getBootstrapPartitionPath(), fileInfo.getPartitionPath(), fileInfo.getBootstrapFileStatus(),
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
