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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.hfile.HFileContext;
import org.apache.hudi.io.hfile.HFileWriter;
import org.apache.hudi.io.hfile.HFileWriterImpl;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex.INDEX_INFO_KEY_STRING;
import static org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex.fileIdIndexPath;
import static org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex.getFileGroupKey;
import static org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex.getPartitionKey;
import static org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex.partitionIndexPath;

public class HFileBootstrapIndexWriter extends BootstrapIndex.IndexWriter {
  private static final Logger LOG = LoggerFactory.getLogger(HFileBootstrapIndexWriter.class);

  private final String bootstrapBasePath;
  private final StoragePath indexByPartitionPath;
  private final StoragePath indexByFileIdPath;
  private HFileWriter indexByPartitionWriter;
  private HFileWriter indexByFileIdWriter;

  private boolean closed = false;
  private int numPartitionKeysAdded = 0;
  private int numFileIdKeysAdded = 0;

  private final Map<String, List<BootstrapFileMapping>> sourceFileMappings = new HashMap<>();

  public HFileBootstrapIndexWriter(String bootstrapBasePath, HoodieTableMetaClient metaClient) {
    super(metaClient);
    try {
      metaClient.initializeBootstrapDirsIfNotExists();
      this.bootstrapBasePath = bootstrapBasePath;
      this.indexByPartitionPath = partitionIndexPath(metaClient);
      this.indexByFileIdPath = fileIdIndexPath(metaClient);

      if (metaClient.getStorage().exists(indexByPartitionPath)
          || metaClient.getStorage().exists(indexByFileIdPath)) {
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
   * @param partitionPath    Hudi Partition Path
   * @param bootstrapPartitionPath  Source Partition Path
   * @param bootstrapFileMappings   Bootstrap Source File to Hudi File Id mapping
   */
  private void writeNextPartition(String partitionPath, String bootstrapPartitionPath,
                                  List<BootstrapFileMapping> bootstrapFileMappings) {
    try {
      LOG.info("Adding bootstrap partition Index entry for partition :" + partitionPath
          + ", bootstrap Partition :" + bootstrapPartitionPath + ", Num Entries :" + bootstrapFileMappings.size());
      LOG.info("ADDING entries :" + bootstrapFileMappings);
      HoodieBootstrapPartitionMetadata bootstrapPartitionMetadata = new HoodieBootstrapPartitionMetadata();
      bootstrapPartitionMetadata.setBootstrapPartitionPath(bootstrapPartitionPath);
      bootstrapPartitionMetadata.setPartitionPath(partitionPath);
      bootstrapPartitionMetadata.setFileIdToBootstrapFile(
          bootstrapFileMappings.stream().map(m -> Pair.of(m.getFileId(),
              m.getBootstrapFileStatus())).collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
      Option<byte[]> bytes = TimelineMetadataUtils.serializeAvroMetadata(bootstrapPartitionMetadata, HoodieBootstrapPartitionMetadata.class);
      if (bytes.isPresent()) {
        indexByPartitionWriter.append(getPartitionKey(partitionPath), bytes.get());
        numPartitionKeysAdded++;
      }
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  /**
   * Write next source file to hudi file-id. Entries are expected to be appended in hudi file-group id
   * order.
   * @param mapping bootstrap source file mapping.
   */
  private void writeNextSourceFileMapping(BootstrapFileMapping mapping) {
    try {
      HoodieBootstrapFilePartitionInfo srcFilePartitionInfo = new HoodieBootstrapFilePartitionInfo();
      srcFilePartitionInfo.setPartitionPath(mapping.getPartitionPath());
      srcFilePartitionInfo.setBootstrapPartitionPath(mapping.getBootstrapPartitionPath());
      srcFilePartitionInfo.setBootstrapFileStatus(mapping.getBootstrapFileStatus());
      indexByFileIdWriter.append(
          getFileGroupKey(mapping.getFileGroupId()),
          TimelineMetadataUtils.serializeAvroMetadata(
              srcFilePartitionInfo, HoodieBootstrapFilePartitionInfo.class).get());
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
        HoodieBootstrapIndexInfo partitionIndexInfo = HoodieBootstrapIndexInfo.newBuilder()
            .setCreatedTimestamp(new Date().getTime())
            .setNumKeys(numPartitionKeysAdded)
            .setBootstrapBasePath(bootstrapBasePath)
            .build();
        LOG.info("Adding Partition FileInfo :" + partitionIndexInfo);

        HoodieBootstrapIndexInfo fileIdIndexInfo = HoodieBootstrapIndexInfo.newBuilder()
            .setCreatedTimestamp(new Date().getTime())
            .setNumKeys(numFileIdKeysAdded)
            .setBootstrapBasePath(bootstrapBasePath)
            .build();
        LOG.info("Appending FileId FileInfo :" + fileIdIndexInfo);

        indexByPartitionWriter.appendFileInfo(
            INDEX_INFO_KEY_STRING,
            TimelineMetadataUtils.serializeAvroMetadata(partitionIndexInfo, HoodieBootstrapIndexInfo.class).get());
        indexByFileIdWriter.appendFileInfo(
            INDEX_INFO_KEY_STRING,
            TimelineMetadataUtils.serializeAvroMetadata(fileIdIndexInfo, HoodieBootstrapIndexInfo.class).get());

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
      HFileContext context = HFileContext.builder().build();
      OutputStream outputStreamForPartitionWriter = metaClient.getStorage().create(indexByPartitionPath);
      this.indexByPartitionWriter = new HFileWriterImpl(context, outputStreamForPartitionWriter);
      OutputStream outputStreamForFileIdWriter = metaClient.getStorage().create(indexByFileIdPath);
      this.indexByFileIdWriter = new HFileWriterImpl(context, outputStreamForFileIdWriter);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  @Override
  public void appendNextPartition(String partitionPath, List<BootstrapFileMapping> bootstrapFileMappings) {
    sourceFileMappings.put(partitionPath, bootstrapFileMappings);
  }

  @Override
  public void finish() {
    // Sort and write
    List<String> partitions = sourceFileMappings.keySet().stream().sorted().collect(Collectors.toList());
    partitions.forEach(p -> writeNextPartition(p, sourceFileMappings.get(p).get(0).getBootstrapPartitionPath(),
        sourceFileMappings.get(p)));
    sourceFileMappings.values().stream().flatMap(Collection::stream).sorted()
        .forEach(this::writeNextSourceFileMapping);
    commit();
  }
}
