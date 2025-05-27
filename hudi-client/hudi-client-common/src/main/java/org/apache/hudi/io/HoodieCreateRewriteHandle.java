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

package org.apache.hudi.io;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.rewrite.HoodieFileMetadataMerger;
import org.apache.hudi.io.storage.rewrite.HoodieFileRewriter;
import org.apache.hudi.io.storage.rewrite.HoodieFileRewriterFactory;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class HoodieCreateRewriteHandle<T, I, K, O> extends HoodieCreateHandle<T, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieCreateRewriteHandle.class);

  protected final HoodieFileRewriter rewriter;

  private final List<StoragePath> inputFiles;

  public HoodieCreateRewriteHandle(
      HoodieWriteConfig config,
      String instantTime,
      String partitionPath,
      String fileId,
      HoodieTable<T, I, K, O> hoodieTable,
      TaskContextSupplier taskContextSupplier,
      List<StoragePath> inputFilePaths,
      boolean preserveMetadata) {
    super(
        config,
        instantTime,
        hoodieTable,
        partitionPath,
        fileId,
        Option.empty(),
        taskContextSupplier,
        preserveMetadata,
        false);
    try {
      this.inputFiles = inputFilePaths;
      HoodieFileMetadataMerger fileMetadataMerger = new HoodieFileMetadataMerger();
      this.rewriter = HoodieFileRewriterFactory.getFileRewriter(
          inputFilePaths,
          path,
          hoodieTable.getStorageConf().unwrapAs(Configuration.class),
          hoodieTable.getConfig(),
          fileMetadataMerger,
          config.getRecordMerger().getRecordType(), this.writeSchemaWithMetaFields);
    } catch (IOException e) {
      LOG.error("Fail to create file rewriter, cause: ", e);
      throw new HoodieException(e);
    }
  }

  public void rewrite() {
    LOG.info("Start to rewrite source files " + this.inputFiles + " into target file: " + this.path);
    long start = System.currentTimeMillis();
    long records = 0;
    try {
      records = this.rewriter.rewrite();
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    } finally {
      this.recordsWritten = records;
      this.insertRecordsWritten = records;
    }
    LOG.info("Finish rewriting " + this.path + ". Using " + (System.currentTimeMillis() - start) + " mills");
  }

  @Override
  public List<WriteStatus> close() {
    try {
      this.rewriter.close();
      return super.close();
    } catch (IOException e) {
      LOG.error("Fail to close the rewrite handle for path: " + path);
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  @Override
  public boolean canWrite(HoodieRecord record) {
    return true;
  }
}
