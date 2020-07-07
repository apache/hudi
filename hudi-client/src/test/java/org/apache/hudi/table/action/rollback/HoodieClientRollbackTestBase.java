package org.apache.hudi.table.action.rollback;

import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.HoodieTestDataGenerator;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class HoodieClientRollbackTestBase extends HoodieClientTestBase {
  protected void twoUpsertCommitDataWithTwoPartitions(List<FileSlice> firstPartitionCommit2FileSlices,
                                                      List<FileSlice> secondPartitionCommit2FileSlices,
                                                      HoodieWriteConfig cfg) throws IOException {
    //just generate two partitions
    dataGen = new HoodieTestDataGenerator(new String[]{DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH});
    //1. prepare data
    HoodieTestDataGenerator.writePartitionMetadata(fs, new String[]{DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH}, basePath);
    HoodieWriteClient client = getHoodieWriteClient(cfg);
    /**
     * Write 1 (only inserts)
     */
    String newCommitTime = "001";
    client.startCommitWithTime(newCommitTime);
    List<HoodieRecord> records = dataGen.generateInsertsContainsAllPartitions(newCommitTime, 2);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
    assertNoWriteErrors(statuses);
    /**
     * Write 2 (updates)
     */
    newCommitTime = "002";
    client.startCommitWithTime(newCommitTime);
    records = dataGen.generateUpdates(newCommitTime, records);
    statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
    assertNoWriteErrors(statuses);
    HoodieTable table = this.getHoodieTable(metaClient, cfg);

    HoodieTableType tableType = this.getTableType();
    //2. assert filegroup and get the first partition fileslice
    List<HoodieFileGroup> firstPartitionCommit2FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_FIRST_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, firstPartitionCommit2FileGroups.size());
    firstPartitionCommit2FileGroups.get(0).getAllFileSlices().collect(Collectors.toList())
        .forEach(fileSlice -> firstPartitionCommit2FileSlices.add(fileSlice));
    //3. assert filegroup and get the second partition fileslice
    List<HoodieFileGroup> secondPartitionCommit2FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_SECOND_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, secondPartitionCommit2FileGroups.size());
    secondPartitionCommit2FileGroups.get(0).getAllFileSlices().collect(Collectors.toList())
        .forEach(fileSlice -> secondPartitionCommit2FileSlices.add(fileSlice));

    //4. assert fileslice
    if (tableType.equals(HoodieTableType.COPY_ON_WRITE)) {
      assertEquals(2, firstPartitionCommit2FileSlices.size());
      assertEquals(2, secondPartitionCommit2FileSlices.size());
    } else {
      assertEquals(1, firstPartitionCommit2FileSlices.size());
      assertEquals(1, secondPartitionCommit2FileSlices.size());
    }

  }
}
