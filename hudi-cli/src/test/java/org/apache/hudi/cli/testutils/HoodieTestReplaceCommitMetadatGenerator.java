package org.apache.hudi.cli.testutils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.util.Option;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.apache.hudi.common.testutils.FileCreateUtils.baseFileName;
import static org.apache.hudi.common.util.CollectionUtils.createImmutableList;

public class HoodieTestReplaceCommitMetadatGenerator extends HoodieTestCommitMetadataGenerator{
    public static void createReplaceCommitFileWithMetadata(String basePath, String commitTime, Configuration configuration,
                                                           Option<Integer> writes, Option<Integer> updates) throws Exception {
        createReplaceCommitFileWithMetadata(basePath, commitTime, configuration, UUID.randomUUID().toString(),
                UUID.randomUUID().toString(), writes, updates);
    }

    private static void createReplaceCommitFileWithMetadata(String basePath, String commitTime, Configuration configuration,
                                                            String fileId1, String fileId2, Option<Integer> writes,
                                                            Option<Integer> updates) throws Exception {
        List<String> commitFileNames = Arrays.asList(HoodieTimeline.makeCommitFileName(commitTime),
                HoodieTimeline.makeInflightCommitFileName(commitTime),
                HoodieTimeline.makeRequestedCommitFileName(commitTime));

        for (String name : commitFileNames) {
            HoodieReplaceCommitMetadata commitMetadata =
                    generateReplaceCommitMetadata(basePath, commitTime, fileId1, fileId2, writes, updates);
            String content = commitMetadata.toJsonString();
            createFileWithMetadata(basePath, configuration, name, content);
        }
    }

    private static HoodieReplaceCommitMetadata generateReplaceCommitMetadata(String basePath, String commitTime, String fileId1, String fileId2, Option<Integer> writes, Option<Integer> updates) throws Exception {
        FileCreateUtils.createBaseFile(basePath, DEFAULT_FIRST_PARTITION_PATH, commitTime, fileId1);
        FileCreateUtils.createBaseFile(basePath, DEFAULT_SECOND_PARTITION_PATH, commitTime, fileId2);
        return generateReplaceCommitMetadata(new HashMap<String, List<String>>() {
            {
                put(DEFAULT_FIRST_PARTITION_PATH, createImmutableList(baseFileName(DEFAULT_FIRST_PARTITION_PATH, fileId1)));
                put(DEFAULT_SECOND_PARTITION_PATH, createImmutableList(baseFileName(DEFAULT_SECOND_PARTITION_PATH, fileId2)));
            }
        }, writes, updates);
    }

    private static HoodieReplaceCommitMetadata generateReplaceCommitMetadata(HashMap<String, List<String>> partitionToFilePaths, Option<Integer> writes, Option<Integer> updates) {
        HoodieReplaceCommitMetadata metadata = new HoodieReplaceCommitMetadata();
        partitionToFilePaths.forEach((key, value) -> value.forEach(f -> {
            HoodieWriteStat writeStat = new HoodieWriteStat();
            writeStat.setPartitionPath(key);
            writeStat.setPath(DEFAULT_PATH);
            writeStat.setFileId(DEFAULT_FILEID);
            writeStat.setTotalWriteBytes(DEFAULT_TOTAL_WRITE_BYTES);
            writeStat.setPrevCommit(DEFAULT_PRE_COMMIT);
            writeStat.setNumWrites(writes.orElse(DEFAULT_NUM_WRITES));
            writeStat.setNumUpdateWrites(updates.orElse(DEFAULT_NUM_UPDATE_WRITES));
            writeStat.setTotalLogBlocks(DEFAULT_TOTAL_LOG_BLOCKS);
            writeStat.setTotalLogRecords(DEFAULT_TOTAL_LOG_RECORDS);
            metadata.addWriteStat(key, writeStat);
        }));
        metadata.setPartitionToReplaceFileIds(new HashMap<String, List<String>>() {
            {
                //TODO fix
                put(DEFAULT_FIRST_PARTITION_PATH, createImmutableList(baseFileName(DEFAULT_FIRST_PARTITION_PATH, "1")));
            }
        });
        return metadata;
    }
}
