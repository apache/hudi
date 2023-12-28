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

package org.apache.hudi.hadoop.realtime;

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.hadoop.HoodieFileGroupReaderRecordReader;
import org.apache.hudi.hadoop.hive.HoodieCombineRealtimeFileSplit;

import org.apache.hadoop.hive.ql.io.IOContextMap;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Allows to read multiple realtime file splits grouped together by CombineInputFormat.
 */
public class HoodieCombineRealtimeRecordReader implements RecordReader<NullWritable, ArrayWritable> {

  private static final transient Logger LOG = LoggerFactory.getLogger(HoodieCombineRealtimeRecordReader.class);
  // RecordReaders for each split
  private List<HoodieRealtimeRecordReader> recordReaders = new LinkedList<>();
  // Points to the currently iterating record reader
  private HoodieRealtimeRecordReader currentRecordReader;

  private final boolean useFileGroupReader;

  // RecordReaders for each split
  private List<HoodieFileGroupReaderRecordReader> recordReadersFG = new LinkedList<>();
  // Points to the currently iterating record reader
  private HoodieFileGroupReaderRecordReader currentRecordReaderFG;

  public HoodieCombineRealtimeRecordReader(JobConf jobConf, CombineFileSplit split,
      List<RecordReader> readers) {
    useFileGroupReader = HoodieFileGroupReaderRecordReader.useFilegroupReader(jobConf);
    if (useFileGroupReader) {
      try {
        ValidationUtils.checkArgument(((HoodieCombineRealtimeFileSplit) split).getRealtimeFileSplits().size() == readers
            .size(), "Num Splits does not match number of unique RecordReaders!");
        for (InputSplit rtSplit : ((HoodieCombineRealtimeFileSplit) split).getRealtimeFileSplits()) {
          LOG.info("Creating new RealtimeRecordReader for split");
          RecordReader reader = readers.remove(0);
          ValidationUtils.checkArgument(reader instanceof HoodieFileGroupReaderRecordReader, reader.toString() + "not instance of HoodieFileGroupReaderRecordReader ");
          recordReadersFG.add((HoodieFileGroupReaderRecordReader) reader);
        }
        currentRecordReaderFG = recordReadersFG.remove(0);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      try {
        ValidationUtils.checkArgument(((HoodieCombineRealtimeFileSplit) split).getRealtimeFileSplits().size() == readers
            .size(), "Num Splits does not match number of unique RecordReaders!");
        for (InputSplit rtSplit : ((HoodieCombineRealtimeFileSplit) split).getRealtimeFileSplits()) {
          LOG.info("Creating new RealtimeRecordReader for split");
          recordReaders.add(
              new HoodieRealtimeRecordReader((HoodieRealtimeFileSplit) rtSplit, jobConf, readers.remove(0)));
        }
        currentRecordReader = recordReaders.remove(0);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public boolean next(NullWritable key, ArrayWritable value) throws IOException {
    if (useFileGroupReader) {
      if (this.currentRecordReaderFG.next(key, value)) {
        return true;
      } else if (recordReadersFG.size() > 0) {
        this.currentRecordReaderFG.close();
        this.currentRecordReaderFG = recordReadersFG.remove(0);
        HoodieFileGroupReaderRecordReader reader = currentRecordReaderFG;
        // when switch reader, ioctx should be updated
        IOContextMap.get(reader.getJobConf()).setInputPath(reader.getSplit().getPath());
        return next(key, value);
      } else {
        return false;
      }
    } else {
      if (this.currentRecordReader.next(key, value)) {
        return true;
      } else if (recordReaders.size() > 0) {
        this.currentRecordReader.close();
        this.currentRecordReader = recordReaders.remove(0);
        AbstractRealtimeRecordReader reader = (AbstractRealtimeRecordReader)currentRecordReader.getReader();
        // when switch reader, ioctx should be updated
        IOContextMap.get(reader.getJobConf()).setInputPath(reader.getSplit().getPath());
        return next(key, value);
      } else {
        return false;
      }
    }
  }

  @Override
  public NullWritable createKey() {
    if (useFileGroupReader) {
      return this.currentRecordReaderFG.createKey();
    } else {
      return this.currentRecordReader.createKey();
    }
  }

  @Override
  public ArrayWritable createValue() {
    if (useFileGroupReader) {
      return this.currentRecordReaderFG.createValue();
    } else {
      return this.currentRecordReader.createValue();
    }
  }

  @Override
  public long getPos() throws IOException {
    if (useFileGroupReader) {
      return this.currentRecordReaderFG.getPos();
    } else {
      return this.currentRecordReader.getPos();
    }
  }

  @Override
  public void close() throws IOException {
    if (useFileGroupReader) {
      this.currentRecordReaderFG.close();
    } else {
      this.currentRecordReader.close();
    }
  }

  @Override
  public float getProgress() throws IOException {
    if (useFileGroupReader) {
      return this.currentRecordReaderFG.getProgress();
    } else {
      return this.currentRecordReader.getProgress();
    }
  }
}
