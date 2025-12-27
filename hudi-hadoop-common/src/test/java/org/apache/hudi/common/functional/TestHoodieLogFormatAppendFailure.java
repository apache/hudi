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

package org.apache.hudi.common.functional;

import org.apache.hudi.common.model.HoodieArchivedLogFile;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorageUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestUtils.shouldUseExternalHdfs;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSimpleSchema;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class TestHoodieLogFormatAppendFailure {

  private static File baseDir;
  private static MiniDFSCluster cluster;

  @BeforeAll
  public static void setUpClass() throws IOException {
    // This test is not supported yet for Java 17 due to MiniDFSCluster can't initialize under Java 17
    Assumptions.assumeFalse(shouldUseExternalHdfs());

    // NOTE : The MiniClusterDFS leaves behind the directory under which the cluster was created
    baseDir = new File("/tmp/" + UUID.randomUUID());
    FileUtil.fullyDelete(baseDir);
    // Append is not supported in LocalFileSystem. HDFS needs to be setup.
    Configuration conf = new Configuration();
    // lower heartbeat interval for fast recognition of DN
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 3000);
    cluster = new MiniDFSCluster.Builder(conf).checkExitOnShutdown(true).numDataNodes(4).build();
  }

  @AfterAll
  public static void tearDownClass() {
    // This test is not supported yet for Java 17 due to MiniDFSCluster can't initialize under Java 17
    Assumptions.assumeFalse(shouldUseExternalHdfs());

    cluster.shutdown(true);
    // Force clean up the directory under which the cluster was created
    FileUtil.fullyDelete(baseDir);
  }

  @Test
  @Timeout(60)
  public void testFailedToGetAppendStreamFromHDFSNameNode()
      throws IOException, URISyntaxException, InterruptedException, TimeoutException {

    // Use some fs like LocalFileSystem, that does not support appends
    String uuid = UUID.randomUUID().toString();
    StoragePath localPartitionPath = new StoragePath("/tmp/");
    FileSystem fileSystem = cluster.getFileSystem();
    HoodieStorage storage = HoodieStorageUtils.getStorage(
        HadoopFSUtils.convertToStoragePath(fileSystem.getWorkingDirectory()),
        HadoopFSUtils.getStorageConf(fileSystem.getConf()));
    StoragePath testPath = new StoragePath(localPartitionPath, uuid);
    storage.createDirectory(testPath);

    // Some data & append.
    List<HoodieRecord> records =
        SchemaTestUtil.generateTestRecords(0, 10).stream().map(HoodieAvroIndexedRecord::new)
            .collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>(2);
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieAvroDataBlock dataBlock =
        new HoodieAvroDataBlock(records, header, HoodieRecord.RECORD_KEY_METADATA_FIELD);

    Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(testPath)
        .withFileExtension(HoodieArchivedLogFile.ARCHIVE_EXTENSION).withFileId("commits")
        .withInstantTime("").withStorage(storage).build();

    writer.appendBlock(dataBlock);
    // get the current log file version to compare later
    int logFileVersion = writer.getLogFile().getLogVersion();
    StoragePath logFilePath = writer.getLogFile().getPath();
    writer.close();

    // Wait for 3 times replication of file
    FileSystem fs = (FileSystem) storage.getFileSystem();
    DFSTestUtil.waitReplication(fs, new Path(logFilePath.toUri()), (short) 3);
    // Shut down all DNs that have the last block location for the file
    LocatedBlocks lbs = cluster.getFileSystem().getClient().getNamenode()
        .getBlockLocations("/tmp/" + uuid + "/" + logFilePath.getName(), 0, Long.MAX_VALUE);
    List<DataNode> dnsOfCluster = cluster.getDataNodes();
    DatanodeInfo[] dnsWithLocations = lbs.getLastLocatedBlock().getLocations();
    for (DataNode dn : dnsOfCluster) {
      for (DatanodeInfo loc : dnsWithLocations) {
        if (dn.getDatanodeId().equals(loc)) {
          dn.shutdown();
          cluster.stopDataNode(dn.getDisplayName());
          DFSTestUtil.waitForDatanodeDeath(dn);
        }
      }
    }
    // Wait for the replication of this file to go down to 0
    DFSTestUtil.waitReplication(fs, new Path(logFilePath.toUri()), (short) 0);

    // Opening a new Writer right now will throw IOException. The code should handle this, rollover the logfile and
    // return a new writer with a bumped up logVersion
    writer = HoodieLogFormat.newWriterBuilder().onParentPath(testPath)
        .withFileExtension(HoodieArchivedLogFile.ARCHIVE_EXTENSION).withFileId("commits")
        .withInstantTime("").withStorage(storage).build();
    header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));

    writer.appendBlock(new HoodieCommandBlock(header));
    // The log version should be different for this new writer
    assertNotEquals(writer.getLogFile().getLogVersion(), logFileVersion);
    writer.close();
  }
}
