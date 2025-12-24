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

package org.apache.hudi.table.format.mor.lsm;

import org.apache.flink.runtime.io.compression.BlockCompressionFactory;
import org.apache.flink.runtime.io.disk.ChannelReaderInputViewIterator;
import org.apache.flink.runtime.io.disk.iomanager.AbstractChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.AbstractChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.io.ChannelWithMeta;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.FileChannelUtil;
import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.MappingIterator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.lsm.ExternalRecordReader;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class FlinkExternalRecordReader implements ExternalRecordReader<RowData, HoodieRecord> {

  protected final Iterator<RowData> iterator;
  protected final RowDataSerializer rowDataSerializer;
  protected final int recordKeyIndex;
  protected final BlockCompressionFactory compressFactory;
  protected final IOManager ioManager;
  protected final int compressBlockSize = 64 * 1024;
  protected final int memorySegmentSize;
  protected AbstractChannelWriterOutputView outputView;
  protected AbstractChannelReaderInputView inputView;

  public FlinkExternalRecordReader(ClosableIterator<RowData> iterator, RowDataSerializer rowDataSerializer,
                                   int recordKeyIndex, IOManager ioManager, int memorySegmentSize) throws IOException {
    this.rowDataSerializer = rowDataSerializer;
    this.recordKeyIndex = recordKeyIndex;
    this.compressFactory = BlockCompressionFactory.createBlockCompressionFactory("zstd");
    this.ioManager = ioManager;
    this.memorySegmentSize = memorySegmentSize;
    this.iterator = spillToDisk(iterator);
  }

  /**
   *  返回FileChannel RecordReader Iterator
   *
   * @return
   * @throws IOException
   */
  @Nullable
  @Override
  public Iterator<HoodieRecord> read() throws IOException {
    return new MappingIterator<>(iterator, record -> {
      return new HoodieFlinkRecord(record.getString(recordKeyIndex), record);
    });
  }

  @Override
  public void close() throws IOException {
    if (inputView != null) {
      inputView.getChannel().closeAndDelete();
      inputView = null;
    }
  }

  // 将数据溢写到FileChannel
  @Override
  public Iterator<RowData> spillToDisk(Iterator<RowData> iterator) throws IOException {
    ValidationUtils.checkArgument(ioManager != null, "IOManager should not be null!");

    ChannelWithMeta channelWithMeta;
    try {
      // write to channel
      FileIOChannel.ID channel = ioManager.createChannel();
      outputView = FileChannelUtil.createOutputView(
          ioManager, channel, true, compressFactory, compressBlockSize, memorySegmentSize);

      // no data, return an empty iterator
      if (iterator.hasNext()) {
        rowDataSerializer.serialize(iterator.next(), outputView);
      } else {
        return new Iterator<RowData>() {
          @Override
          public boolean hasNext() {
            return false;
          }

          @Override
          public RowData next() {
            return null;
          }
        };
      }

      while (iterator.hasNext()) {
        rowDataSerializer.serialize(iterator.next(), outputView);
      }

      int bytesInLastBlock = outputView.close();
      channelWithMeta = new ChannelWithMeta(channel, outputView.getBlockCount(), bytesInLastBlock);
    } catch (Exception e) {
      // close and delete current file channel
      if (outputView != null) {
        outputView.getChannel().closeAndDelete();
        outputView = null;
      }
      throw new HoodieException(e.getMessage(), e);
    } finally {
      if (iterator instanceof ClosableIterator) {
        ((ClosableIterator<RowData>) iterator).close();
      }
    }

    try {
      inputView = FileChannelUtil.createInputView(
          ioManager, channelWithMeta, new ArrayList<>(), true, compressFactory, compressBlockSize, memorySegmentSize);
      ChannelReaderInputViewIterator inputViewIterator = new ChannelReaderInputViewIterator(inputView, null, rowDataSerializer);
      return new ChannelReaderInputViewIteratorWrapper(inputViewIterator);
    } catch (Exception e) {
      // close and delete current file channel
      close();
      throw new HoodieException(e.getMessage(), e);
    }
  }
}
