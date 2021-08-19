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

package org.apache.hudi.client.clustering.update.strategy;

import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.strategy.UpdateStrategy;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Update strategy based on following.
 * If some file group have update record, Then let clustering job failing.
 *
 * When update happened after clustering plan created and before clustering executed.
 *      -> There will be a request replace commit.
 *      -> SparkRejectClusteringStrategy will create a clustering reject file under .tmp dir named xxx.replacement.request.reject.
 *      -> Before perform clustering job, hudi can check this reject file using SparkRejectClusteringStrategy.validateClustering() function.
 *        -> if reject file is exists then abort this clustering plan and remove reject file.
 * When update happened after clustering executed but not finished.
 *      -> There will be a inflight replace commit.
 *      -> SparkRejectClusteringStrategy will create a clustering reject file under .tmp dir named xxx.replacement.inflight.reject.
 *      -> Before clustering job finished and committed, hudi can check this reject file using SparkRejectClusteringStrategy.validateClustering() function.
 *        -> if reject file is exists then failed this clustering execution and remove reject file.
 */
public class SparkRejectClusteringStrategy<T extends HoodieRecordPayload<T>> extends UpdateStrategy<T, JavaRDD<HoodieRecord<T>>> {
  private static final Logger LOG = LogManager.getLogger(SparkRejectClusteringStrategy.class);
  private static final String REJECTION_FLAG = "reject";

  public SparkRejectClusteringStrategy(HoodieSparkEngineContext engineContext, HashSet<HoodieFileGroupId> fileGroupsInPendingClustering) {
    super(engineContext, fileGroupsInPendingClustering);
  }

  private List<HoodieFileGroupId> getGroupIdsWithUpdate(JavaRDD<HoodieRecord<T>> inputRecords) {
    List<HoodieFileGroupId> fileGroupIdsWithUpdates = inputRecords
        .filter(record -> record.getCurrentLocation() != null)
        .map(record -> new HoodieFileGroupId(record.getPartitionPath(), record.getCurrentLocation().getFileId())).distinct().collect();
    return fileGroupIdsWithUpdates;
  }

  private void createRejectPath(Path rejectFile, HoodieWrapperFileSystem fs) {
    Path tmpDir = rejectFile.getParent();
    try {
      if (!fs.exists(tmpDir)) {
        fs.mkdirs(tmpDir); // create a new partition as needed.
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to make dir " + tmpDir, e);
    }

    try {
      if (fs.exists(rejectFile)) {
        LOG.warn("Clustering Reject Path=" + rejectFile + " already exists, cancel creation");
        return;
      }
      LOG.info("Clustering Reject Path=" + rejectFile);
      fs.create(rejectFile, false).close();
      boolean exists = fs.exists(rejectFile);
      System.out.println();
    } catch (IOException e) {
      throw new HoodieException("Failed to create clustering reject file " + rejectFile, e);
    }
  }

  @Override
  public JavaRDD<HoodieRecord<T>> handleUpdate(JavaRDD<HoodieRecord<T>> taggedRecordsRDD, HoodieTable table) {
    boolean conflict = false;
    HashSet<HoodieFileGroupId> hoodieFileGroupIds = new HashSet<>();
    List<HoodieFileGroupId> fileGroupIdsWithRecordUpdate = getGroupIdsWithUpdate(taggedRecordsRDD);
    for (HoodieFileGroupId fileGroupIdWithRecordUpdate: fileGroupIdsWithRecordUpdate) {
      if (fileGroupsInPendingClustering.contains(fileGroupIdWithRecordUpdate)) {
        String msg = String.format("Update happened for the pending clustering file group %s. "
                        + "We will let the clustering job failing.",
                fileGroupIdWithRecordUpdate.toString());
        hoodieFileGroupIds.add(fileGroupIdWithRecordUpdate);
        LOG.warn(msg);
        conflict = true;
      }
    }

    if (conflict) {
      // make reject file in ./tmp dir which will let clustering job failing down.
      HoodieWrapperFileSystem fs = table.getMetaClient().getFs();
      Set<Path> rejectPaths = table.getFileSystemView()
              .getFileGroupsInPendingClustering()
              .filter(entry -> hoodieFileGroupIds.contains(entry.getKey()))
              .map(Pair::getValue)
              .map(clusteringHoodieInstant -> {
                String rejectPath = getClusteringRejectFilePath(table, clusteringHoodieInstant);
                return new Path(rejectPath);
              })
              .collect(Collectors.toSet());
      LOG.info("Start to create clustering reject files : " + rejectPaths);
      for (Path rejectFile : rejectPaths) {
        createRejectPath(rejectFile, fs);
      }
    }
    return taggedRecordsRDD;
  }

  private String getClusteringRejectFilePath(HoodieTable table, HoodieInstant clusteringHoodieInstant) {
    String tempFolderPath = table.getMetaClient().getTempFolderPath();
    String fileName = clusteringHoodieInstant.getFileName();
    return String.format("%s%s%s%s%s", tempFolderPath, Path.SEPARATOR, fileName, Path.CUR_DIR, REJECTION_FLAG);
  }

  @Override
  public boolean validateClustering(HoodieInstant instant, HoodieTable table) {
    // check if exists in ./tmp dir
    boolean updatedDuringClustering = false;
    String clusteringRejectFilePath = getClusteringRejectFilePath(table, instant);
    Path clusteringRejectFile = new Path(clusteringRejectFilePath);
    boolean clusteringRejectFileExists = checkClusteringRejectFileExists(clusteringRejectFile, table);

    if (clusteringRejectFileExists) {
      updatedDuringClustering = true;
      postAction(clusteringRejectFile, table);
    }
    return !updatedDuringClustering;
  }

  /**
   * For now just delete this reject file which let clustering job failed.
   * In the future, We can use this post action to correct clustering plan like remove the updated file slice in clustering plan and overwrite with a new one.
   * @param clusteringRejectFile
   * @param table
   */
  private void postAction(Path clusteringRejectFile, HoodieTable table) {
    HoodieWrapperFileSystem fs = table.getMetaClient().getFs();

    try {
      if (fs.exists(clusteringRejectFile)) {
        table.getMetaClient().getFs().delete(clusteringRejectFile, true);
      }
      LOG.info("Deleted clustering reject file " + clusteringRejectFile);
    } catch (IOException e) {
      LOG.warn("Failed to delete clustering reject file " + clusteringRejectFile, e);
    }
  }

  private boolean checkClusteringRejectFileExists(Path clusteringRejectFilePath, HoodieTable table) {
    try {
      return table.getMetaClient().getFs().exists(clusteringRejectFilePath);
    } catch (IOException e) {
      LOG.warn("Exceptions when check clustering reject file : " + clusteringRejectFilePath, e);
      return false;
    }
  }
}
