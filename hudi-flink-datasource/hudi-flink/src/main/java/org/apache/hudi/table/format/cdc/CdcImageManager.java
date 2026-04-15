/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.format.cdc;

import lombok.Getter;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.format.FormatUtils;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.HOODIE_RECORD_KEY_COL_POS;

/**
 * Manages serialized before/after image snapshots for a CDC file group, cached by instant time.
 *
 * <p>At most two versions (before and after) are kept in memory at once; older entries are
 * evicted and spilled to disk via {@link ExternalSpillableMap}.
 *
 * <p>Also owns the I/O-view adapters ({@link BytesArrayInputView} / {@link BytesArrayOutputView})
 * used for serialising {@link RowData} records into byte arrays.
 */
public class CdcImageManager implements AutoCloseable {
  @Getter
  private final HoodieWriteConfig writeConfig;
  private final RowDataSerializer serializer;
  private final Function<MergeOnReadInputSplit, ClosableIterator<RowData>> splitIteratorFunc;
  private final Map<String, ExternalSpillableMap<String, byte[]>> cache;

  public CdcImageManager(
      RowType rowType,
      HoodieWriteConfig writeConfig,
      Function<MergeOnReadInputSplit, ClosableIterator<RowData>> splitIteratorFunc) {
    this.serializer = new RowDataSerializer(rowType);
    this.writeConfig = writeConfig;
    this.splitIteratorFunc = splitIteratorFunc;
    this.cache = new TreeMap<>();
  }

  public ExternalSpillableMap<String, byte[]> getOrLoadImages(
      long maxCompactionMemoryInBytes,
      FileSlice fileSlice) throws IOException {
    final String instant = fileSlice.getBaseInstantTime();
    if (cache.containsKey(instant)) {
      return cache.get(instant);
    }
    // evict the earliest version when more than two are cached (keep before & after)
    if (cache.size() > 1) {
      String oldest = cache.keySet().iterator().next();
      cache.remove(oldest).close();
    }
    ExternalSpillableMap<String, byte[]> images = loadImageRecords(maxCompactionMemoryInBytes, fileSlice);
    cache.put(instant, images);
    return images;
  }

  private ExternalSpillableMap<String, byte[]> loadImageRecords(
      long maxCompactionMemoryInBytes,
      FileSlice fileSlice) throws IOException {
    MergeOnReadInputSplit inputSplit = CdcIterators.fileSlice2Split(
        writeConfig.getBasePath(), fileSlice, maxCompactionMemoryInBytes);
    ExternalSpillableMap<String, byte[]> imageRecordsMap =
        FormatUtils.spillableMap(writeConfig, maxCompactionMemoryInBytes, getClass().getSimpleName());
    try (ClosableIterator<RowData> itr = splitIteratorFunc.apply(inputSplit)) {
      while (itr.hasNext()) {
        RowData row = itr.next();
        String recordKey = row.getString(HOODIE_RECORD_KEY_COL_POS).toString();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
        serializer.serialize(row, new BytesArrayOutputView(baos));
        imageRecordsMap.put(recordKey, baos.toByteArray());
      }
    }
    return imageRecordsMap;
  }

  public RowData getImageRecord(
      String recordKey,
      ExternalSpillableMap<String, byte[]> imageCache,
      RowKind rowKind) {
    byte[] bytes = imageCache.get(recordKey);
    ValidationUtils.checkState(bytes != null,
        "Key " + recordKey + " does not exist in current file group image");
    try {
      RowData row = serializer.deserialize(new BytesArrayInputView(bytes));
      row.setRowKind(rowKind);
      return row;
    } catch (IOException e) {
      throw new HoodieException("Failed to deserialize image record for key: " + recordKey, e);
    }
  }

  public void updateImageRecord(
      String recordKey,
      ExternalSpillableMap<String, byte[]> imageCache,
      RowData row) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
    try {
      serializer.serialize(row, new BytesArrayOutputView(baos));
    } catch (IOException e) {
      throw new HoodieException("Failed to serialize image record for key: " + recordKey, e);
    }
    imageCache.put(recordKey, baos.toByteArray());
  }

  public RowData removeImageRecord(
      String recordKey,
      ExternalSpillableMap<String, byte[]> imageCache) {
    byte[] bytes = imageCache.remove(recordKey);
    if (bytes == null) {
      return null;
    }
    try {
      return serializer.deserialize(new BytesArrayInputView(bytes));
    } catch (IOException e) {
      throw new HoodieException("Failed to deserialize image record for key: " + recordKey, e);
    }
  }

  @Override
  public void close() {
    cache.values().forEach(ExternalSpillableMap::close);
    cache.clear();
  }

  // -------------------------------------------------------------------------
  //  I/O view adapters for RowDataSerializer
  // -------------------------------------------------------------------------

  static final class BytesArrayInputView extends DataInputStream implements DataInputView {
    BytesArrayInputView(byte[] data) {
      super(new ByteArrayInputStream(data));
    }

    @Override
    public void skipBytesToRead(int numBytes) throws IOException {
      while (numBytes > 0) {
        int skipped = skipBytes(numBytes);
        numBytes -= skipped;
      }
    }
  }

  static final class BytesArrayOutputView extends DataOutputStream implements DataOutputView {
    BytesArrayOutputView(ByteArrayOutputStream baos) {
      super(baos);
    }

    @Override
    public void skipBytesToWrite(int numBytes) throws IOException {
      for (int i = 0; i < numBytes; i++) {
        write(0);
      }
    }

    @Override
    public void write(DataInputView source, int numBytes) throws IOException {
      byte[] buffer = new byte[numBytes];
      source.readFully(buffer);
      write(buffer);
    }
  }
}
