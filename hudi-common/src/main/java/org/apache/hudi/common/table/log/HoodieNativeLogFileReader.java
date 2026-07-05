/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemas;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.log.block.HoodieNativeDataBlock;
import org.apache.hudi.common.table.log.block.HoodieNativeDeleteBlock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.storage.HoodieStorage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Reader for native log files.
 */
public class HoodieNativeLogFileReader implements HoodieLogFormat.Reader {

  private final HoodieStorage storage;
  private final HoodieLogFile logFile;
  private final HoodieReaderContext<?> readerContext;
  private final HoodieSchema readerSchema;
  private final List<String> orderingFieldNames;
  private boolean consumed;

  HoodieNativeLogFileReader(HoodieStorage storage, HoodieLogFile logFile, HoodieReaderContext<?> readerContext,
                            HoodieSchema readerSchema, List<String> orderingFieldNames) {
    this.storage = storage;
    this.logFile = logFile;
    this.readerContext = readerContext;
    this.readerSchema = readerSchema;
    this.orderingFieldNames = orderingFieldNames;
  }

  @Override
  public HoodieLogFile getLogFile() {
    return logFile;
  }

  @Override
  public boolean hasPrev() {
    return false;
  }

  @Override
  public HoodieLogBlock prev() {
    throw new HoodieNotSupportedException("Reverse reading is not supported for native log files");
  }

  @Override
  public void close() {
    // no-op
  }

  @Override
  public boolean hasNext() {
    return !consumed;
  }

  @Override
  public HoodieLogBlock next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more blocks in native log file " + logFile);
    }
    consumed = true;
    HoodieFileFormat fileFormat = getNativeFileFormat();
    Map<HeaderMetadataType, String> header = getLogBlockHeader(fileFormat);
    if (FSUtils.isNativeDeleteLogFile(logFile.getFileName())) {
      HoodieSchema deleteLogSchema = HoodieSchemas.createDeleteLogSchema(
          readerContext.getSchemaHandler().getTableSchema(), orderingFieldNames);
      return new HoodieNativeDeleteBlock(storage, logFile, readerContext, deleteLogSchema,
          orderingFieldNames, header, new HashMap<>());
    }
    validateDataBlockHeader(logFile, header);
    return new HoodieNativeDataBlock(storage, logFile, fileFormat, Option.ofNullable(readerSchema), header, new HashMap<>());
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Remove not supported for HoodieNativeLogFileReader");
  }

  private Map<HeaderMetadataType, String> getLogBlockHeader(HoodieFileFormat fileFormat) {
    Map<String, String> keyValueMetadata = HoodieIOFactory.getIOFactory(storage)
        .getFileFormatUtils(fileFormat)
        .readFooter(storage, false, logFile.getPath(), NativeLogFooterMetadata.FOOTER_METADATA_KEY);
    Map<HeaderMetadataType, String> header = NativeLogFooterMetadata.fromFooterMetadata(keyValueMetadata);
    header.putIfAbsent(HeaderMetadataType.INSTANT_TIME, logFile.getDeltaCommitTime());
    return header;
  }

  static void validateDataBlockHeader(HoodieLogFile logFile, Map<HeaderMetadataType, String> header) {
    if (!header.containsKey(HeaderMetadataType.SCHEMA)) {
      throw new HoodieIOException("Missing required native log block header metadata '"
          + HeaderMetadataType.SCHEMA.name() + "' for " + logFile
          + ". Native data logs must store log block metadata in footer key '"
          + NativeLogFooterMetadata.FOOTER_METADATA_KEY + "'");
    }
  }

  private HoodieFileFormat getNativeFileFormat() {
    String suffix = logFile.getSuffix();
    try {
      return HoodieFileFormat.fromFileExtension("." + suffix);
    } catch (IllegalArgumentException e) {
      throw new HoodieNotSupportedException(
          "Unsupported native log file format suffix '" + suffix + "' for " + logFile);
    }
  }
}
