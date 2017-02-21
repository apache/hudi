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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import com.uber.hoodie.common.model.HoodieCommits;
import com.uber.hoodie.common.model.HoodieTableMetadata;
import com.uber.hoodie.common.util.FSUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Hoodie snapshot copy job which copies latest files from all partitions to another place, for snapshot backup.
 */
public class HoodieSnapshotCopier implements Serializable {
    private static Logger logger = LogManager.getLogger(HoodieSnapshotCopier.class);

    static class Config implements Serializable {
        @Parameter(names = {"--base-path", "-bp"}, description = "Hoodie table base path", required = true)
        String basePath = null;

        @Parameter(names = {"--output-path", "-op"}, description = "The snapshot output path", required = true)
        String outputPath = null;
    }

    public void snapshot(JavaSparkContext jsc, String baseDir, final String outputDir) throws IOException {
        FileSystem fs = FSUtils.getFs();
        final HoodieTableMetadata tableMetadata = new HoodieTableMetadata(fs, baseDir);

        // Get the latest commit
        final String latestCommit = tableMetadata.getAllCommits().lastCommit();
        logger.info(String.format("Starting to snapshot latest version files which are also no-late-than %s.", latestCommit));

        List<String> partitions = FSUtils.getAllPartitionPaths(fs, baseDir);
        if (partitions.size() > 0) {
            logger.info(String.format("The job needs to copy %d partitions.", partitions.size()));

            // Make sure the output directory is empty
            Path outputPath = new Path(outputDir);
            if (fs.exists(outputPath)) {
                logger.warn(String.format("The output path %s already exists, deleting", outputPath));
                fs.delete(new Path(outputDir), true);
            }

            jsc.parallelize(partitions, partitions.size()).flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
                @Override
                public Iterator<Tuple2<String, String>> call(String partition) throws Exception {
                    // Only take latest version files <= latestCommit.
                    FileSystem fs = FSUtils.getFs();
                    List<Tuple2<String, String>> filePaths = new ArrayList<>();
                    for (FileStatus fileStatus : tableMetadata.getLatestVersionInPartition(fs, partition, latestCommit)) {
                        filePaths.add(new Tuple2<>(partition, fileStatus.getPath().toString()));
                    }
                    return filePaths.iterator();
                }
            }).foreach(new VoidFunction<Tuple2<String, String>>() {
                @Override
                public void call(Tuple2<String, String> tuple) throws Exception {
                    String partition = tuple._1();
                    Path sourceFilePath = new Path(tuple._2());
                    Path toPartitionPath = new Path(outputDir, partition);
                    FileSystem fs = FSUtils.getFs();

                    if (!fs.exists(toPartitionPath)) {
                        fs.mkdirs(toPartitionPath);
                    }
                    FileUtil.copy(fs, sourceFilePath, fs, new Path(toPartitionPath, sourceFilePath.getName()),
                            false, fs.getConf());
                }
            });

            // Also copy the .commit files
            logger.info(String.format("Copying .commit files which are no-late-than %s.", latestCommit));
            FileStatus[] commitFilesToCopy = fs.listStatus(
                    new Path(baseDir + "/" + HoodieTableMetadata.METAFOLDER_NAME), new PathFilter() {
                @Override
                public boolean accept(Path commitFilePath) {
                    if (commitFilePath.getName().equals(HoodieTableMetadata.HOODIE_PROPERTIES_FILE)) {
                        return true;
                    } else {
                        String commitTime = FSUtils.getCommitFromCommitFile(commitFilePath.getName());
                        return HoodieCommits.isCommit1BeforeOrOn(commitTime, latestCommit);
                    }
                }
            });
            for (FileStatus commitStatus : commitFilesToCopy) {
                Path targetFilePath =
                        new Path(outputDir + "/" + HoodieTableMetadata.METAFOLDER_NAME + "/" + commitStatus.getPath().getName());
                if (! fs.exists(targetFilePath.getParent())) {
                    fs.mkdirs(targetFilePath.getParent());
                }
                if (fs.exists(targetFilePath)) {
                    logger.error(String.format("The target output commit file (%s) already exists.", targetFilePath));
                }
                FileUtil.copy(fs, commitStatus.getPath(), fs, targetFilePath, false, fs.getConf());
            }
        } else {
            logger.info("The job has 0 partition to copy.");
        }

        // Create the _SUCCESS tag
        Path successTagPath = new Path(outputDir + "/_SUCCESS");
        if (!fs.exists(successTagPath)) {
            logger.info(String.format("Creating _SUCCESS under %s.", outputDir));
            fs.createNewFile(successTagPath);
        }
    }

    public static void main(String[] args) throws IOException {
        // Take input configs
        final Config cfg = new Config();
        new JCommander(cfg, args);
        logger.info(String.format("Snapshot hoodie table from %s to %s", cfg.basePath, cfg.outputPath));

        // Create a spark job to do the snapshot copy
        SparkConf sparkConf = new SparkConf().setAppName("Hoodie-snapshot-copier");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        logger.info("Initializing spark job.");

        // Copy
        HoodieSnapshotCopier copier = new HoodieSnapshotCopier();
        copier.snapshot(jsc, cfg.basePath, cfg.outputPath);

        // Stop the job
        jsc.stop();
    }
}
