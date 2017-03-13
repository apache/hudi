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

package com.uber.hoodie.cli.commands;

import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.cli.DedupeSparkJob;
import com.uber.hoodie.cli.utils.SparkUtil;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.index.HoodieIndex;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.util.Date;

public class SparkMain {

    protected final static Logger LOG = Logger.getLogger(SparkMain.class);


    /**
     * Commands
     */
    enum SparkCommand {
        ROLLBACK,
        DEDUPLICATE,
        ROLLBACK_TO_SAVEPOINT, SAVEPOINT
    }

    public static void main(String[] args) throws Exception {
        String command = args[0];
        LOG.info("Invoking SparkMain:" + command);

        SparkCommand cmd = SparkCommand.valueOf(command);

        JavaSparkContext jsc = SparkUtil.initJavaSparkConf("hoodie-cli-" + command);
        int returnCode = 0;
        if (SparkCommand.ROLLBACK.equals(cmd)) {
            assert (args.length == 3);
            returnCode = rollback(jsc, args[1], args[2]);
        } else if(SparkCommand.DEDUPLICATE.equals(cmd)) {
            assert (args.length == 4);
            returnCode = deduplicatePartitionPath(jsc, args[1], args[2], args[3]);
        } else if(SparkCommand.ROLLBACK_TO_SAVEPOINT.equals(cmd)) {
            assert (args.length == 3);
            returnCode = rollbackToSavepoint(jsc, args[1], args[2]);
        }

        System.exit(returnCode);
    }

    private static int deduplicatePartitionPath(JavaSparkContext jsc,
                                                String duplicatedPartitionPath,
                                                String repairedOutputPath,
                                                String basePath)
        throws Exception {
        DedupeSparkJob job = new DedupeSparkJob(basePath,
                duplicatedPartitionPath,repairedOutputPath,new SQLContext(jsc), FSUtils.getFs());
        job.fixDuplicates(true);
        return 0;
    }

    private static int rollback(JavaSparkContext jsc, String commitTime, String basePath)
        throws Exception {
        HoodieWriteClient client = createHoodieClient(jsc, basePath);
        if (client.rollback(commitTime)) {
            LOG.info(String.format("The commit \"%s\" rolled back.", commitTime));
            return 0;
        } else {
            LOG.info(String.format("The commit \"%s\" failed to roll back.", commitTime));
            return -1;
        }
    }

    private static int rollbackToSavepoint(JavaSparkContext jsc, String savepointTime, String basePath)
        throws Exception {
        HoodieWriteClient client = createHoodieClient(jsc, basePath);
        if (client.rollbackToSavepoint(savepointTime)) {
            LOG.info(String.format("The commit \"%s\" rolled back.", savepointTime));
            return 0;
        } else {
            LOG.info(String.format("The commit \"%s\" failed to roll back.", savepointTime));
            return -1;
        }
    }

    private static HoodieWriteClient createHoodieClient(JavaSparkContext jsc, String basePath)
        throws Exception {
        HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
            .withIndexConfig(
                HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
            .build();
        return new HoodieWriteClient(jsc, config);
    }
}
