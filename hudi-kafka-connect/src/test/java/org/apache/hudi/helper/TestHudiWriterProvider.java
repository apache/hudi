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

package org.apache.hudi.helper;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.connect.writers.ConnectWriter;
import org.apache.hudi.connect.writers.ConnectWriterProvider;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;

/**
 * Helper class the provides a Hudi writer and
 * maintains stats that are used for test validation.
 */
@NoArgsConstructor
public class TestHudiWriterProvider implements ConnectWriterProvider<WriteStatus>  {

  private TestHudiWriter currentWriter;

  public int getLatestNumberWrites() {
    return (currentWriter != null) ? currentWriter.numberRecords : 0;
  }

  public boolean isClosed() {
    return currentWriter == null || currentWriter.isClosed;
  }

  @Override
  public ConnectWriter<WriteStatus> getWriter(String commitTime) {
    currentWriter = new TestHudiWriter();
    return currentWriter;
  }

  @Getter
  private static class TestHudiWriter implements ConnectWriter<WriteStatus> {

    private int numberRecords;
    private boolean isClosed;

    public TestHudiWriter() {
      this.numberRecords = 0;
      this.isClosed = false;
    }

    @Override
    public void writeRecord(SinkRecord record) {
      numberRecords++;
    }

    @Override
    public List<WriteStatus> close() {
      isClosed = false;
      return null;
    }
  }
}
