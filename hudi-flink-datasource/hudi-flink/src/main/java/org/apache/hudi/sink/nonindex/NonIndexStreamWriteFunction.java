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

package org.apache.hudi.sink.nonindex;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.sink.StreamWriteFunction;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.util.StreamerUtil;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Used for MOR table if index type is non_index.
 */
public class NonIndexStreamWriteFunction<I>
    extends StreamWriteFunction<I> {

  private static final Logger LOG = LogManager.getLogger(NonIndexStreamWriteFunction.class);

  private transient Map<String, FileGroupInfo> partitionPathToFileId;

  private transient HoodieWriteConfig writeConfig;

  private transient PartitionFileGroupHandle partitionFileGroupHandle;

  /**
   * Constructs a StreamingSinkFunction.
   *
   * @param config The config options
   */
  public NonIndexStreamWriteFunction(Configuration config) {
    super(config);
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    super.initializeState(context);
    partitionPathToFileId = new ConcurrentHashMap<>();
    writeConfig = StreamerUtil.getHoodieClientConfig(this.config);
    partitionFileGroupHandle = getPartitionHandle(PartitionFileGroupStorageType.valueOf(writeConfig.getNonIndexPartitionFileGroupStorageType()));
    partitionFileGroupHandle.init();
  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
    super.snapshotState(functionSnapshotContext);
    partitionFileGroupHandle.reset();
  }

  @Override
  public void processElement(I value, ProcessFunction<I, Object>.Context context, Collector<Object> collector) throws Exception {
    HoodieRecord<?> record = (HoodieRecord<?>) value;
    assignFileId(record);
    bufferRecord(record);
  }

  private void assignFileId(HoodieRecord<?> record) {
    String partitionPath = record.getPartitionPath();
    FileGroupInfo fileGroupInfo = partitionPathToFileId.computeIfAbsent(partitionPath,
        key -> getLatestFileGroupInfo(record.getPartitionPath()));
    if (fileGroupInfo == null || !fileGroupInfo.canAssign(record)) {
      fileGroupInfo = new FileGroupInfo(FSUtils.getFileId(FSUtils.createNewFileIdPfx()));
      partitionPathToFileId.put(partitionPath, fileGroupInfo);
    }
    record.unseal();
    record.setCurrentLocation(new HoodieRecordLocation("U", fileGroupInfo.getFileId()));
    record.seal();
  }

  private FileGroupInfo getLatestFileGroupInfo(String partitionPath) {
    return partitionFileGroupHandle.getLatestFileGroupInfo(partitionPath);
  }

  @Override
  public void endInput() {
    super.endInput();
    partitionPathToFileId.clear();
  }

  private abstract class PartitionFileGroupHandle {

    abstract void init();

    abstract FileGroupInfo getLatestFileGroupInfo(String partitionPath);

    abstract void reset();

  }

  private class InMemoryPartitionFileGroupHandle extends PartitionFileGroupHandle {

    private long cacheIntervalMills;

    @Override
    public void init() {
      cacheIntervalMills = writeConfig.getNonIndexPartitionFileGroupCacheIntervalMinute() * 60 * 1000;
    }

    @Override
    public FileGroupInfo getLatestFileGroupInfo(String partitionPath) {
      return null;
    }

    @Override
    public void reset() {
      long curTime = System.currentTimeMillis();
      for (Entry<String, FileGroupInfo> entry : partitionPathToFileId.entrySet()) {
        if (curTime - entry.getValue().getCreateTime() >= cacheIntervalMills) {
          partitionPathToFileId.remove(entry.getKey());
        }
      }
    }
  }

  @Override
  protected boolean shouldFlushBucket(BufferSizeDetector detector, DataItem item, String partitionPath) {
    FileGroupInfo fileGroupInfo = partitionPathToFileId.get(partitionPath);
    return fileGroupInfo == null || fileGroupInfo.getLastRecordSize() == -1 ? detector.detect(item)
        : detector.detect(fileGroupInfo.getLastRecordSize());
  }

  private class FileSystemPartitionFileGroupHandle extends PartitionFileGroupHandle {

    private String writeToken;

    private HoodieTable<?, ?, ?, ?> table;

    private HoodieFlinkEngineContext engineContext;

    private SyncableFileSystemView fileSystemView;

    private boolean needSync = false;

    @Override
    public void init() {
      this.engineContext = new HoodieFlinkEngineContext(
          new SerializableConfiguration(HadoopConfigurations.getHadoopConf(config)),
          new FlinkTaskContextSupplier(getRuntimeContext()));
      this.writeToken = FSUtils.makeWriteToken(
          engineContext.getTaskContextSupplier().getPartitionIdSupplier().get(),
          engineContext.getTaskContextSupplier().getStageIdSupplier().get(),
          engineContext.getTaskContextSupplier().getAttemptIdSupplier().get());
      this.table = HoodieFlinkTable.create(writeConfig, engineContext);
      this.fileSystemView = table.getHoodieView();
    }

    @Override
    public FileGroupInfo getLatestFileGroupInfo(String partitionPath) {
      if (needSync) {
        fileSystemView.sync();
        needSync = false;
      }
      List<HoodieLogFile> hoodieLogFiles = fileSystemView.getAllFileSlices(partitionPath)
          .map(FileSlice::getLatestLogFile)
          .filter(Option::isPresent).map(Option::get)
          .filter(logFile -> FSUtils.getWriteTokenFromLogPath(logFile.getPath()).equals(writeToken))
          .sorted(HoodieLogFile.getReverseLogFileComparator()).collect(Collectors.toList());
      if (hoodieLogFiles.size() > 0) {
        HoodieLogFile hoodieLogFile = hoodieLogFiles.get(0);
        Option<FileSlice> fileSlice = fileSystemView.getLatestFileSlice(partitionPath, hoodieLogFile.getFileId());
        if (fileSlice.isPresent()) {
          return new FileGroupInfo(FSUtils.getFileIdFromFilePath(hoodieLogFile.getPath()),
              fileSlice.get().getFileGroupSize());
        }
        LOG.warn("Can location fileSlice, partitionPath: " + partitionPath + ", fileId: "
            + hoodieLogFile.getFileId());
      }
      return null;
    }

    @Override
    public void reset() {
      partitionPathToFileId.clear();
      needSync = true;
    }
  }

  private PartitionFileGroupHandle getPartitionHandle(PartitionFileGroupStorageType storageType) {
    switch (storageType) {
      case IN_MEMORY:
        return new InMemoryPartitionFileGroupHandle();
      case FILE_SYSTEM:
        return new FileSystemPartitionFileGroupHandle();
      default:
        throw new IllegalArgumentException("UnSupport storage type :" + storageType.name());
    }
  }

  private enum PartitionFileGroupStorageType {
    IN_MEMORY, FILE_SYSTEM
  }

  private class FileGroupInfo {

    private final String fileId;
    private final long createTime;
    private final BufferSizeDetector detector;

    public FileGroupInfo(String fileId) {
      this.fileId = fileId;
      this.createTime = System.currentTimeMillis();
      this.detector = new BufferSizeDetector((double) writeConfig.getNonIndexPartitionFileGroupCacheSize() / 1024 / 1024);
    }

    public FileGroupInfo(String fileId, long initFileSize) {
      this(fileId);
      detector.setTotalSize(initFileSize);
    }

    public boolean canAssign(HoodieRecord<?> record) {
      return !detector.detect(record);
    }

    public String getFileId() {
      return fileId;
    }

    public long getCreateTime() {
      return createTime;
    }

    public long getLastRecordSize() {
      return detector.getLastRecordSize();
    }
  }

}
