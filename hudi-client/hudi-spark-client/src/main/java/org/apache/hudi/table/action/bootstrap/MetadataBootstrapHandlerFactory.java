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

package org.apache.hudi.table.action.bootstrap;

import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.bootstrap.FileStatusUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.avro.model.HoodieFileStatus;
import static org.apache.hudi.common.model.HoodieFileFormat.ORC;
import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;

public class MetadataBootstrapHandlerFactory {

  public static BootstrapMetadataHandler getMetadataHandler(HoodieWriteConfig config, HoodieTable table, HoodieFileStatus srcFileStatus) {
    Path sourceFilePath = FileStatusUtils.toPath(srcFileStatus.getPath());

    String extension = FSUtils.getFileExtension(sourceFilePath.toString());
    BootstrapMetadataHandler bootstrapMetadataHandler;
    if (ORC.getFileExtension().equals(extension)) {
      return new OrcBootstrapMetadataHandler(config, table, srcFileStatus);
    } else if (PARQUET.getFileExtension().equals(extension)) {
      return new ParquetBootstrapMetadataHandler(config, table, srcFileStatus);
    } else {
      throw new HoodieIOException("Bootstrap Metadata Handler not implemented for base file format " + extension);
    }
  }
}
