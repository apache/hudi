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
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.io.hfile.HFileReader;
import org.apache.hudi.io.hfile.HFileReaderImpl;
import org.apache.hudi.io.hfile.Key;
import org.apache.hudi.io.hfile.UTF8StringKey;
import org.apache.hudi.io.util.IOUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex.INDEX_INFO_KEY_STRING;
import static org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex.fileIdIndexPath;
import static org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex.getFileGroupKey;
import static org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex.getPartitionKey;
import static org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex.partitionIndexPath;
import static org.apache.hudi.common.util.DeserializationUtils.deserializeHoodieBootstrapFilePartitionInfo;
import static org.apache.hudi.common.util.DeserializationUtils.deserializeHoodieBootstrapIndexInfo;
import static org.apache.hudi.common.util.DeserializationUtils.deserializeHoodieBootstrapPartitionMetadata;

/**
 * HFile Based Index Reader.
 */
public class HFileBootstrapIndexReader extends BootstrapIndex.IndexReader {
  private static final Logger LOG = LoggerFactory.getLogger(HFileBootstrapIndexReader.class);

  // Base Path of external files.
  private final String bootstrapBasePath;
  // Well Known Paths for indices
  private final String indexByPartitionPath;
  private final String indexByFileIdPath;

  // Index Readers
  private transient HFileReader indexByPartitionReader;
  private transient HFileReader indexByFileIdReader;

  // Bootstrap Index Info
  private transient HoodieBootstrapIndexInfo bootstrapIndexInfo;

  public HFileBootstrapIndexReader(HoodieTableMetaClient metaClient) {
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
   * Helper method to create native HFile Reader.
   *
   * @param hFilePath file path.
   * @param storage   {@link HoodieStorage} instance.
   */
  private static HFileReader createReader(String hFilePath, HoodieStorage storage) throws IOException {
    LOG.info("Opening HFile for reading :" + hFilePath);
    StoragePath path = new StoragePath(hFilePath);
    long fileSize = storage.getPathInfo(path).getLength();
    SeekableDataInputStream stream = storage.openSeekable(path, false);
    return new HFileReaderImpl(stream, fileSize);
  }

  private synchronized void initIndexInfo() {
    if (bootstrapIndexInfo == null) {
      try {
        bootstrapIndexInfo = fetchBootstrapIndexInfo();
      } catch (IOException ioe) {
        throw new HoodieException(ioe.getMessage(), ioe);
      }
    }
  }

  private HoodieBootstrapIndexInfo fetchBootstrapIndexInfo() throws IOException {
    return deserializeHoodieBootstrapIndexInfo(
        partitionIndexReader().getMetaInfo(new UTF8StringKey(INDEX_INFO_KEY_STRING)).get());
  }

  private synchronized HFileReader partitionIndexReader() throws IOException {
    if (indexByPartitionReader == null) {
      LOG.info("Opening partition index :" + indexByPartitionPath);
      this.indexByPartitionReader = createReader(indexByPartitionPath, metaClient.getStorage());
    }
    return indexByPartitionReader;
  }

  private synchronized HFileReader fileIdIndexReader() throws IOException {
    if (indexByFileIdReader == null) {
      LOG.info("Opening fileId index :" + indexByFileIdPath);
      this.indexByFileIdReader = createReader(indexByFileIdPath, metaClient.getStorage());
    }
    return indexByFileIdReader;
  }

  @Override
  public List<String> getIndexedPartitionPaths() {
    try {
      return getAllKeys(partitionIndexReader(), HFileBootstrapIndex::getPartitionFromKey);
    } catch (IOException e) {
      throw new HoodieIOException("Unable to read indexed partition paths.", e);
    }
  }

  @Override
  public List<HoodieFileGroupId> getIndexedFileGroupIds() {
    try {
      return getAllKeys(fileIdIndexReader(), HFileBootstrapIndex::getFileGroupFromKey);
    } catch (IOException e) {
      throw new HoodieIOException("Unable to read indexed file group IDs.", e);
    }
  }

  private <T> List<T> getAllKeys(HFileReader reader, Function<String, T> converter) {
    List<T> keys = new ArrayList<>();
    try {
      boolean available = reader.seekTo();
      while (available) {
        keys.add(converter.apply(reader.getKeyValue().get().getKey().getContentInString()));
        available = reader.next();
      }
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }

    return keys;
  }

  @Override
  public List<BootstrapFileMapping> getSourceFileMappingForPartition(String partition) {
    try {
      HFileReader reader = partitionIndexReader();
      Key lookupKey = new UTF8StringKey(getPartitionKey(partition));
      reader.seekTo();
      if (reader.seekTo(lookupKey) == HFileReader.SEEK_TO_FOUND) {
        org.apache.hudi.io.hfile.KeyValue keyValue = reader.getKeyValue().get();
        byte[] valBytes = IOUtils.copy(
            keyValue.getBytes(), keyValue.getValueOffset(), keyValue.getValueLength());
        HoodieBootstrapPartitionMetadata metadata = deserializeHoodieBootstrapPartitionMetadata(valBytes);
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
    try {
      HFileReader reader = fileIdIndexReader();
      reader.seekTo();
      for (HoodieFileGroupId fileGroupId : fileGroupIds) {
        Key lookupKey = new UTF8StringKey(getFileGroupKey(fileGroupId));
        if (reader.seekTo(lookupKey) == HFileReader.SEEK_TO_FOUND) {
          org.apache.hudi.io.hfile.KeyValue keyValue = reader.getKeyValue().get();
          byte[] valBytes = IOUtils.copy(
              keyValue.getBytes(), keyValue.getValueOffset(), keyValue.getValueLength());
          HoodieBootstrapFilePartitionInfo fileInfo = deserializeHoodieBootstrapFilePartitionInfo(valBytes);
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
        indexByPartitionReader.close();
        indexByPartitionReader = null;
      }
      if (indexByFileIdReader != null) {
        indexByFileIdReader.close();
        indexByFileIdReader = null;
      }
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }
}
