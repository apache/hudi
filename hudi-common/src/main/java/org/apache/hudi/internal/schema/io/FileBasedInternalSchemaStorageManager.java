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

package org.apache.hudi.internal.schema.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.InternalSchemaUtils;
import org.apache.hudi.internal.schema.utils.SerDeHelper;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.SCHEMA_COMMIT_ACTION;

public class FileBasedInternalSchemaStorageManager extends AbstractInternalSchemaStorageManager {
  private static final Logger LOG = LogManager.getLogger(FileBasedInternalSchemaStorageManager.class);

  public static final String SCHEMA_NAME = ".schema";
  private final Path baseSchemaPath;
  private final Configuration conf;
  private HoodieTableMetaClient metaClient;

  public FileBasedInternalSchemaStorageManager(Configuration conf, Path baseTablePath) {
    Path metaPath = new Path(baseTablePath, ".hoodie");
    this.baseSchemaPath = new Path(metaPath, SCHEMA_NAME);
    this.conf = conf;
  }

  public FileBasedInternalSchemaStorageManager(HoodieTableMetaClient metaClient) {
    Path metaPath = new Path(metaClient.getBasePath(), ".hoodie");
    this.baseSchemaPath = new Path(metaPath, SCHEMA_NAME);
    this.conf = metaClient.getHadoopConf();
    this.metaClient = metaClient;
  }

  // make metaClient build lazy
  private HoodieTableMetaClient getMetaClient() {
    if (metaClient == null) {
      metaClient = HoodieTableMetaClient.builder().setBasePath(baseSchemaPath.getParent().getParent().toString()).setConf(conf).build();
    }
    return metaClient;
  }

  @Override
  public void persistHistorySchemaStr(String instantTime, String historySchemaStr) {
    cleanResidualFiles();
    HoodieActiveTimeline timeline = getMetaClient().getActiveTimeline();
    HoodieInstant hoodieInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, SCHEMA_COMMIT_ACTION, instantTime);
    timeline.createNewInstant(hoodieInstant);
    byte[] writeContent = historySchemaStr.getBytes(StandardCharsets.UTF_8);
    timeline.transitionRequestedToInflight(hoodieInstant, Option.empty());
    timeline.saveAsComplete(new HoodieInstant(HoodieInstant.State.INFLIGHT, hoodieInstant.getAction(), hoodieInstant.getTimestamp()), Option.of(writeContent));
    LOG.info(String.format("persist history schema success on commit time: %s", instantTime));
  }

  private void cleanResidualFiles() {
    List<String> validateCommits = getValidInstants();
    try {
      FileSystem fs = baseSchemaPath.getFileSystem(conf);
      if (fs.exists(baseSchemaPath)) {
        List<String> candidateSchemaFiles = Arrays.stream(fs.listStatus(baseSchemaPath)).filter(f -> f.isFile())
            .map(file -> file.getPath().getName()).collect(Collectors.toList());
        List<String> residualSchemaFiles = candidateSchemaFiles.stream().filter(f -> !validateCommits.contains(f.split("\\.")[0])).collect(Collectors.toList());
        // clean residual files
        residualSchemaFiles.forEach(f -> {
          try {
            fs.delete(new Path(getMetaClient().getSchemaFolderName(), f));
          } catch (IOException o) {
            throw new HoodieException(o);
          }
        });
      }
    } catch (IOException e) {
      throw new HoodieException(e);
    }
  }

  public void cleanOldFiles(List<String> validateCommits) {
    try {
      FileSystem fs = baseSchemaPath.getFileSystem(conf);
      if (fs.exists(baseSchemaPath)) {
        List<String> candidateSchemaFiles = Arrays.stream(fs.listStatus(baseSchemaPath)).filter(f -> f.isFile())
            .map(file -> file.getPath().getName()).collect(Collectors.toList());
        List<String> validateSchemaFiles = candidateSchemaFiles.stream().filter(f -> validateCommits.contains(f.split("\\.")[0])).collect(Collectors.toList());
        for (int i = 0; i < validateSchemaFiles.size(); i++) {
          fs.delete(new Path(validateSchemaFiles.get(i)));
        }
      }
    } catch (IOException e) {
      throw new HoodieException(e);
    }
  }

  private List<String> getValidInstants() {
    return getMetaClient().getCommitsTimeline()
        .filterCompletedInstants().getInstants().map(f -> f.getTimestamp()).collect(Collectors.toList());
  }

  @Override
  public String getHistorySchemaStr() {
    return getHistorySchemaStrByGivenValidCommits(Collections.EMPTY_LIST);
  }

  @Override
  public String getHistorySchemaStrByGivenValidCommits(List<String> validCommits) {
    List<String> commitList = validCommits == null || validCommits.isEmpty() ? getValidInstants() : validCommits;
    try {
      FileSystem fs = FSUtils.getFs(baseSchemaPath.toString(), conf);
      if (fs.exists(baseSchemaPath)) {
        List<String> validaSchemaFiles = Arrays.stream(fs.listStatus(baseSchemaPath))
            .filter(f -> f.isFile() && f.getPath().getName().endsWith(SCHEMA_COMMIT_ACTION))
            .map(file -> file.getPath().getName()).filter(f -> commitList.contains(f.split("\\.")[0])).sorted().collect(Collectors.toList());
        if (!validaSchemaFiles.isEmpty()) {
          Path latestFilePath = new Path(baseSchemaPath, validaSchemaFiles.get(validaSchemaFiles.size() - 1));
          byte[] content;
          try (FSDataInputStream is = fs.open(latestFilePath)) {
            content = FileIOUtils.readAsByteArray(is);
            LOG.info(String.format("read history schema success from file : %s", latestFilePath));
            return new String(content, StandardCharsets.UTF_8);
          } catch (IOException e) {
            throw new HoodieIOException("Could not read history schema from " + latestFilePath, e);
          }
        }
      }
    } catch (IOException io) {
      throw new HoodieException(io);
    }
    LOG.info("failed to read history schema");
    return "";
  }

  @Override
  public Option<InternalSchema> getSchemaByKey(String versionId) {
    String historySchemaStr = getHistorySchemaStr();
    TreeMap<Long, InternalSchema> treeMap;
    if (historySchemaStr.isEmpty()) {
      return Option.empty();
    } else {
      treeMap = SerDeHelper.parseSchemas(historySchemaStr);
      InternalSchema result = InternalSchemaUtils.searchSchema(Long.valueOf(versionId), treeMap);
      if (result == null) {
        return Option.empty();
      }
      return Option.of(result);
    }
  }
}


