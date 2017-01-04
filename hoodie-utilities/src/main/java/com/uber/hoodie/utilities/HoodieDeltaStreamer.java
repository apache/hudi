/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.utilities;

import com.google.common.io.Files;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.common.HoodieJsonPayload;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.index.HoodieIndex;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An Utility which can incrementally take the output from {@link HiveIncrementalPuller} and apply it to the target dataset.
 * Does not maintain any state, queries at runtime to see how far behind the target dataset is from
 * the source dataset. This can be overriden to force sync from a timestamp.
 */
public class HoodieDeltaStreamer implements Serializable {
    private static volatile Logger log = LogManager.getLogger(HoodieDeltaStreamer.class);
    private final Config cfg;

    public HoodieDeltaStreamer(Config cfg) throws IOException {
        this.cfg = cfg;
    }

    private void sync() throws Exception {
        JavaSparkContext sc = getSparkContext(cfg);
        FileSystem fs = FSUtils.getFs();
        HoodieTableMetaClient targetHoodieMetadata = new HoodieTableMetaClient(fs, cfg.targetPath);
        HoodieTimeline timeline = targetHoodieMetadata.getActiveCommitTimeline();
        String lastCommitPulled = findLastCommitPulled(fs, cfg.dataPath);
        log.info("Last commit pulled on the source dataset is " + lastCommitPulled);
        if (!timeline.getInstants().iterator().hasNext() && timeline
            .compareInstants(timeline.lastInstant().get(), lastCommitPulled,
                HoodieTimeline.GREATER)) {
            // this should never be the case
            throw new IllegalStateException(
                "Last commit pulled from source table " + lastCommitPulled
                    + " is before the last commit in the target table " + timeline.lastInstant()
                    .get());
        }
        if (!cfg.override && timeline.containsOrBeforeTimelineStarts(lastCommitPulled)) {
            throw new IllegalStateException(
                "Target Table already has the commit " + lastCommitPulled
                    + ". Not overriding as cfg.override is false");
        }
        syncTill(lastCommitPulled, targetHoodieMetadata, sc);
    }

    private String findLastCommitPulled(FileSystem fs, String dataPath) throws IOException {
        FileStatus[] commitTimePaths = fs.listStatus(new Path(dataPath));
        List<String> commitTimes = new ArrayList<>(commitTimePaths.length);
        for (FileStatus commitTimePath : commitTimePaths) {
            String[] splits = commitTimePath.getPath().toString().split("/");
            commitTimes.add(splits[splits.length - 1]);
        }
        Collections.sort(commitTimes);
        Collections.reverse(commitTimes);
        log.info("Retrieved commit times " + commitTimes);
        return commitTimes.get(0);
    }

    private void syncTill(String lastCommitPulled, HoodieTableMetaClient target,
                          JavaSparkContext sc) throws Exception {
        // Step 1 : Scan incrementally and get the input records as a RDD of source format
        String dataPath = cfg.dataPath + "/" + lastCommitPulled;
        log.info("Using data path " + dataPath);
        JavaRDD<String> rdd = sc.textFile(dataPath);

        // Step 2 : Create the hoodie records
        JavaRDD<HoodieRecord<HoodieJsonPayload>> records =
                rdd.map(new Function<String, HoodieRecord<HoodieJsonPayload>>() {
                    @Override
                    public HoodieRecord<HoodieJsonPayload> call(String json)
                            throws Exception {
                        HoodieJsonPayload payload = new HoodieJsonPayload(json);
                        HoodieKey key = new HoodieKey(payload.getRowKey(cfg.keyColumnField),
                                payload.getPartitionPath(cfg.partitionPathField));
                        return new HoodieRecord<>(key, payload);
                    }
                });

        // Step 3: Use Hoodie Client to upsert/bulk load the records into target hoodie dataset
        HoodieWriteConfig hoodieCfg = getHoodieClientConfig(target);
        HoodieWriteClient<HoodieJsonPayload> client = new HoodieWriteClient<>(sc, hoodieCfg);
        log.info("Rollback started " + lastCommitPulled);
        client.rollback(lastCommitPulled);

        client.startCommitWithTime(lastCommitPulled);
        log.info("Starting commit " + lastCommitPulled);
        if (cfg.upsert) {
            log.info("Upserting records");
            client.upsert(records, lastCommitPulled);
        } else {
            log.info("Inserting records");
            // insert the records.
            client.insert(records, lastCommitPulled);
        }

        // TODO - revisit this - can we clean this up.
        // determine if this write should be committed.
//        final Accumulator<Integer> errorCount = sc.intAccumulator(0);
//        final Accumulator<Integer> totalCount = sc.intAccumulator(0);
//        statuses.foreach(new VoidFunction<WriteStatus>() {
//            @Override public void call(WriteStatus status) throws Exception {
//                if (status.hasGlobalError()) {
//                    log.error(status.getGlobalError());
//                    errorCount.add(1);
//                }
//                if (status.hasErrors()) {
//                    log.info(status);
//                    for (Map.Entry<HoodieKey, Throwable> keyErrEntry : status.getErrors()
//                        .entrySet()) {
//                        log.error(String.format("\t %s error %s", keyErrEntry.getKey(),
//                            keyErrEntry.getValue().getMessage()), keyErrEntry.getValue());
//                    }
//                }
//                errorCount.add(status.getErrors().size());
//                totalCount.add(status.getWrittenRecords().size());
//            }
//        })
    }

    private HoodieWriteConfig getHoodieClientConfig(HoodieTableMetaClient metadata)
            throws Exception {
        final String schemaStr = Files.toString(new File(cfg.schemaFile), Charset.forName("UTF-8"));
        return HoodieWriteConfig.newBuilder().withPath(metadata.getBasePath())
                .withSchema(schemaStr)
                .withParallelism(cfg.groupByParallelism, cfg.groupByParallelism)
                .forTable(metadata.getTableConfig().getTableName()).withIndexConfig(
                        HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
                .build();
    }

    private JavaSparkContext getSparkContext(Config cfg) {
        SparkConf sparkConf = new SparkConf().setAppName("hoodie-delta-streamer-" + cfg.targetTableName);
        sparkConf.setMaster(cfg.sparkMaster);
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.driver.maxResultSize", "2g");

        if (cfg.sparkMaster.startsWith("yarn")) {
            sparkConf.set("spark.eventLog.overwrite", "true");
            sparkConf.set("spark.eventLog.enabled", "true");
        }

        // Configure hadoop conf
        sparkConf.set("spark.hadoop.mapred.output.compress", "true");
        sparkConf.set("spark.hadoop.mapred.output.compression.codec", "true");
        sparkConf.set("spark.hadoop.mapred.output.compression.codec",
                "org.apache.hadoop.io.compress.GzipCodec");
        sparkConf.set("spark.hadoop.mapred.output.compression.type", "BLOCK");

        sparkConf = HoodieWriteClient.registerClasses(sparkConf);
        return new JavaSparkContext(sparkConf);
    }

    public static class Config implements Serializable {
        @Parameter(names = {"--dataPath"})
        public String dataPath;
        @Parameter(names = {"--parallelism"})
        public int groupByParallelism = 10000;
        @Parameter(names = {"--upsert"})
        public boolean upsert = false;
        @Parameter(names = {"--master"})
        public String sparkMaster = "yarn-client";
        @Parameter(names = {"--targetPath"}, required = true)
        public String targetPath;
        @Parameter(names = {"--targetTable"})
        public String targetTableName;
        @Parameter(names = {"--keyColumn"})
        public String keyColumnField = "uuid";
        @Parameter(names = {"--partitionPathField"})
        public String partitionPathField = "request_at";
        @Parameter(names = {"--schemaFile"})
        public String schemaFile;
        @Parameter(names = {"--override"})
        public boolean override = false;
        @Parameter(names = {"--help", "-h"}, help = true)
        public Boolean help = false;
    }

    public static void main(String[] args) throws Exception {
        final Config cfg = new Config();
        JCommander cmd = new JCommander(cfg, args);
        if (cfg.help || args.length == 0) {
            cmd.usage();
            System.exit(1);
        }
        new HoodieDeltaStreamer(cfg).sync();
    }
}
