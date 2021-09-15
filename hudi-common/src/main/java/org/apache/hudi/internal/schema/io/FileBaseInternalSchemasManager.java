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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.SerDeHelper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class FileBaseInternalSchemasManager extends InternalSchemasManager {
  public static final String SCHEMA_NAME = ".schema";
  private final Path baseSchemaPath;
  private Configuration conf;
  private HoodieTableMetaClient metaClient;

  public FileBaseInternalSchemasManager(Configuration conf, Path baseTablePath) {
    Path metaPath = new Path(baseTablePath, ".hoodie");
    this.baseSchemaPath = new Path(metaPath, SCHEMA_NAME);
    this.conf = conf;
    this.metaClient = HoodieTableMetaClient.builder().setBasePath(metaPath.getParent().toString()).setConf(conf).build();
  }

  public FileBaseInternalSchemasManager(HoodieTableMetaClient metaClient) {
    Path metaPath = new Path(metaClient.getBasePath(), ".hoodie");
    this.baseSchemaPath = new Path(metaPath, SCHEMA_NAME);
    this.conf = metaClient.getHadoopConf();
    this.metaClient = metaClient;
  }

  @Override
  public void persistHistorySchemaStr(String instantTime, String historySchemaStr) {
    Path savePath = new Path(baseSchemaPath, instantTime);
    Path saveTempPath = new Path(baseSchemaPath, instantTime + java.util.UUID.randomUUID().toString());
    try {
      cleanOldFiles();
      byte[] writeContent = historySchemaStr.getBytes(StandardCharsets.UTF_8);
      if (!metaClient.getFs().exists(saveTempPath)) {
        if (!metaClient.getFs().createNewFile(saveTempPath)) {
          throw new HoodieIOException("Failed to create file " + saveTempPath);
        }
      }
      FSDataOutputStream fsout = metaClient.getFs().create(saveTempPath, true);
      fsout.write(writeContent);
      fsout.close();
      // try to rename
      boolean success = metaClient.getFs().rename(saveTempPath, savePath);
      if (!success) {
        throw new HoodieIOException(String.format("cannot rename %s to %s", saveTempPath, savePath));
      }
    } catch (IOException e) {
      throw new HoodieException(e);
    }
  }

  private void cleanOldFiles() {
    List<String> validateCommits = getValidateCommits();
    try {
      FileSystem fs = baseSchemaPath.getFileSystem(conf);
      if (fs.exists(baseSchemaPath)) {
        List<String> candidateSchemaFiles = Arrays.stream(fs.listStatus(baseSchemaPath)).filter(f -> f.isFile())
            .map(file -> file.getPath().getName().split("\\.")[0]).collect(Collectors.toList());
        List<String> validateSchemaFiles = candidateSchemaFiles.stream().filter(f -> validateCommits.contains(f)).collect(Collectors.toList());
        List<String> residualSchemaFiles = candidateSchemaFiles.stream().filter(f -> !validateCommits.contains(f)).collect(Collectors.toList());
        // clean residual files
        residualSchemaFiles.forEach(f -> {
          try {
            fs.delete(new Path(f));
          } catch (IOException o) {
            throw new HoodieException(o);
          }
        });
        // clean old files, keep at most ten schema files
        if (validateSchemaFiles.size() > 10) {
          for (int i = 0; i < validateSchemaFiles.size(); i++) {
            if (i >= 10) {
              break;
            }
            fs.delete(new Path(validateSchemaFiles.get(i)));
          }
        }
      }
    } catch (IOException e) {
      throw new HoodieException(e);
    }
  }

  private List getValidateCommits() {
    metaClient.reloadActiveTimeline();
    return metaClient.getCommitsTimeline()
        .filterCompletedInstants().getInstants().map(f -> f.getTimestamp()).collect(Collectors.toList());
  }

  @Override
  public String getHistorySchemaStr() {
    List<String> validateCommits = getValidateCommits();
    try {
      if (metaClient.getFs().exists(baseSchemaPath)) {
        List<String> validateSchemaFiles = Arrays.stream(metaClient.getFs().listStatus(baseSchemaPath)).filter(f -> f.isFile())
            .map(file -> file.getPath().getName().split("\\.")[0]).filter(f -> validateCommits.contains(f)).sorted().collect(Collectors.toList());
        if (!validateSchemaFiles.isEmpty()) {
          Path latestFilePath = new Path(baseSchemaPath, validateSchemaFiles.get(validateSchemaFiles.size() - 1));
          byte[] content;
          try (FSDataInputStream is = metaClient.getFs().open(latestFilePath)) {
            content = FileIOUtils.readAsByteArray(is);
            return new String(content, StandardCharsets.UTF_8);
          } catch (IOException e) {
            throw new HoodieIOException("Could not read history schema from " + latestFilePath, e);
          }
        }
      }
    } catch (IOException io) {
      throw new HoodieException(io);
    }
    return "";
  }

  @Override
  public Option getSchemaByKey(String versionId) {
    String historySchemaStr = getHistorySchemaStr();
    TreeMap<Long, InternalSchema> treeMap = new TreeMap<>();
    if (historySchemaStr.isEmpty()) {
      return Option.empty();
    } else {
      treeMap = SerDeHelper.parseSchemas(historySchemaStr);
      InternalSchema result = SerDeHelper.searchSchema(Long.valueOf(versionId), treeMap);
      if (result == null) {
        return Option.empty();
      }
      return Option.of(result);
    }
  }
}

