/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.io.compact;

import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieAvroPayload;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.view.RealtimeTableView;
import com.uber.hoodie.common.util.AvroUtils;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieCommitException;
import com.uber.hoodie.table.HoodieCopyOnWriteTable;
import org.apache.avro.Schema;
import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * HoodieRealtimeTableCompactor compacts a hoodie table with merge on read storage.
 * Computes all possible compactions, passes it through a CompactionFilter and executes
 * all the compactions and writes a new version of base files and make a normal commit
 *
 * @see HoodieCompactor
 */
public class HoodieRealtimeTableCompactor implements HoodieCompactor {
    private static Logger log = LogManager.getLogger(HoodieRealtimeTableCompactor.class);

    @Override
    public HoodieCompactionMetadata compact(JavaSparkContext jsc, HoodieWriteConfig config,
        HoodieTableMetaClient metaClient, RealtimeTableView fsView,
        CompactionFilter compactionFilter) throws Exception {
        // TODO - rollback any compactions in flight

        String compactionCommit = startCompactionCommit(metaClient);
        log.info("Compacting " + metaClient.getBasePath() + " with commit " + compactionCommit);
        List<String> partitionPaths =
            FSUtils.getAllPartitionPaths(metaClient.getFs(), metaClient.getBasePath());

        log.info("Compaction looking for files to compact in " + partitionPaths + " partitions");
        List<CompactionOperation> operations =
            jsc.parallelize(partitionPaths, partitionPaths.size())
                .flatMap((FlatMapFunction<String, CompactionOperation>) partitionPath -> {
                    FileSystem fileSystem = FSUtils.getFs();
                    return fsView.groupLatestDataFileWithLogFiles(fileSystem, partitionPath)
                        .entrySet().stream()
                        .map(s -> new CompactionOperation(s.getKey(), partitionPath, s.getValue()))
                        .collect(Collectors.toList());
                }).collect();
        log.info("Total of " + operations.size() + " compactions are retrieved");

        // Filter the compactions with the passed in filter. This lets us choose most effective compactions only
        operations = compactionFilter.filter(operations);
        if(operations.isEmpty()) {
            log.warn("After filtering, Nothing to compact for " + metaClient.getBasePath());
            return null;
        }

        log.info("After filtering, Compacting " + operations + " files");
        List<Tuple2<String, HoodieWriteStat>> updateStatusMap =
            jsc.parallelize(operations, operations.size()).map(
                (Function<CompactionOperation, Iterator<List<WriteStatus>>>) compactionOperation -> executeCompaction(
                    metaClient, config, compactionOperation, compactionCommit)).flatMap(
                (FlatMapFunction<Iterator<List<WriteStatus>>, WriteStatus>) listIterator -> {
                    List<List<WriteStatus>> collected = IteratorUtils.toList(listIterator);
                    return collected.stream().flatMap(List::stream).collect(Collectors.toList());
                }).mapToPair(new PairFunction<WriteStatus, String, HoodieWriteStat>() {
                @Override
                public Tuple2<String, HoodieWriteStat> call(WriteStatus writeStatus)
                    throws Exception {
                    return new Tuple2<>(writeStatus.getPartitionPath(), writeStatus.getStat());
                }
            }).collect();

        HoodieCompactionMetadata metadata = new HoodieCompactionMetadata();
        for (Tuple2<String, HoodieWriteStat> stat : updateStatusMap) {
            metadata.addWriteStat(stat._1(), stat._2());
        }
        log.info("Compaction finished with result " + metadata);

        //noinspection ConstantConditions
        if (isCompactionSucceeded(metadata)) {
            log.info("Compaction succeeded " + compactionCommit);
            commitCompaction(compactionCommit, metaClient, metadata);
        } else {
            log.info("Compaction failed " + compactionCommit);
        }
        return metadata;
    }

    private boolean isCompactionSucceeded(HoodieCompactionMetadata result) {
        //TODO figure out a success factor for a compaction
        return true;
    }

    private Iterator<List<WriteStatus>> executeCompaction(HoodieTableMetaClient metaClient,
        HoodieWriteConfig config, CompactionOperation operation, String commitTime)
        throws IOException {
        FileSystem fs = FSUtils.getFs();
        Schema schema =
            HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));

        log.info("Compacting base " + operation.getDataFilePath() + " with delta files " + operation
            .getDeltaFilePaths() + " for commit " + commitTime);
        // TODO - FIX THIS
        // 1. Reads the entire avro file. Always only specific blocks should be read from the avro file (failure recover).
        // Load all the delta commits since the last compaction commit and get all the blocks to be loaded and load it using CompositeAvroLogReader
        // Since a DeltaCommit is not defined yet, reading all the records. revisit this soon.

        // 2. naively loads all the delta records in memory to merge it,
        // since we only need a iterator, we could implement a lazy iterator to load from one delta file at a time
        List<HoodieRecord<HoodieAvroPayload>> readDeltaFilesInMemory =
            AvroUtils.loadFromFiles(fs, operation.getDeltaFilePaths(), schema);

        HoodieCopyOnWriteTable<HoodieAvroPayload> table =
            new HoodieCopyOnWriteTable<>(commitTime, config, metaClient);
        return table.handleUpdate(operation.getFileId(), readDeltaFilesInMemory.iterator());
    }

    public boolean commitCompaction(String commitTime, HoodieTableMetaClient metaClient,
        HoodieCompactionMetadata metadata) {
        log.info("Comitting " + commitTime);
        HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

        try {
            activeTimeline.saveAsComplete(
                new HoodieInstant(true, HoodieTimeline.COMPACTION_ACTION, commitTime),
                Optional.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
        } catch (IOException e) {
            throw new HoodieCommitException(
                "Failed to commit " + metaClient.getBasePath() + " at time " + commitTime, e);
        }
        return true;
    }

}
