package org.apache.hudi.cli.commands;


import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.HoodieTestCommitMetadataGenerator;
import org.apache.hudi.client.HoodieTimelineArchiver;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;

import org.junit.jupiter.api.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

@Tag("functional")
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestArchiveCommand extends CLIFunctionalTestHarness {

  @Autowired
  private Shell shell;
  private String tablePath;

  @Test
  public void testArchiving() throws Exception {
    HoodieCLI.conf = hadoopConf();

    // Create table and connect
    String tableName = tableName();
    tablePath = tablePath(tableName);

    new TableCommand().createTable(
        tablePath, tableName,
        "COPY_ON_WRITE", "", 1, "org.apache.hudi.common.model.HoodieAvroPayload");

    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();

    // Generate archive
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
        .withSchema(HoodieTestCommitMetadataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(2, 3).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .forTable("test-trip-table").build();

    // Create six commits
    for (int i = 100; i < 106; i++) {
      String timestamp = String.valueOf(i);
      // Requested Compaction
      HoodieTestCommitMetadataGenerator.createCompactionAuxiliaryMetadata(tablePath,
          new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, timestamp), hadoopConf());
      // Inflight Compaction
      HoodieTestCommitMetadataGenerator.createCompactionAuxiliaryMetadata(tablePath,
          new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, timestamp), hadoopConf());
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath, timestamp, hadoopConf());
    }

    // Simulate a compaction commit in metadata table timeline
    // so the archival in data table can happen
    HoodieTestUtils.createCompactionCommitInMetadataTable(
        hadoopConf(), metaClient.getFs(), tablePath, "105");

    metaClient = HoodieTableMetaClient.reload(metaClient);
    // reload the timeline and get all the commits before archive
    metaClient.getActiveTimeline().reload().getAllCommitsTimeline().filterCompletedInstants();

    // archive
    HoodieSparkTable table = HoodieSparkTable.create(cfg, context(), metaClient);
    HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(cfg, table);
    archiver.archiveIfRequired(context());
  }


}
