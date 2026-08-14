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

package org.apache.hudi.table.lookup;

import org.apache.hudi.common.function.SerializableSupplier;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.CloseableUtils.closeSuppressing;

/**
 * Hudi look up table reader.
 */
public class HoodieLookupTableReader implements Serializable, Closeable {
  private static final long serialVersionUID = 1L;

  private final SerializableSupplier<InputFormat<RowData, ?>> inputFormatSupplier;
  private final Configuration conf;

  private InputFormat inputFormat;

  private List<InputSplit> inputSplits;

  public HoodieLookupTableReader(SerializableSupplier<InputFormat<RowData, ?>> inputFormatSupplier, Configuration conf) {
    this.inputFormatSupplier = inputFormatSupplier;
    this.conf = conf;
  }

  public void open() throws IOException {
    close();
    this.inputFormat = inputFormatSupplier.get();
    try {
      inputFormat.configure(conf);
      this.inputSplits = Arrays.stream(inputFormat.createInputSplits(1)).collect(Collectors.toList());
      ((RichInputFormat) inputFormat).openInputFormat();
      inputFormat.open(inputSplits.remove(0));
    } catch (IOException | RuntimeException e) {
      closeSuppressing(this, e);
      throw e;
    }
  }

  @Nullable
  public RowData read(RowData reuse) throws IOException {
    if (!inputFormat.reachedEnd()) {
      return (RowData) inputFormat.nextRecord(reuse);
    } else {
      while (!inputSplits.isEmpty()) {
        // release the last itr first.
        inputFormat.close();
        inputFormat.open(inputSplits.remove(0));
        if (!inputFormat.reachedEnd()) {
          return (RowData) inputFormat.nextRecord(reuse);
        }
      }
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    InputFormat format = this.inputFormat;
    this.inputFormat = null;
    this.inputSplits = null;
    if (format == null) {
      return;
    }

    if (format instanceof RichInputFormat) {
      try (Closeable ignored = ((RichInputFormat) format)::closeInputFormat) {
        format.close();
      }
    } else {
      format.close();
    }
  }
}
