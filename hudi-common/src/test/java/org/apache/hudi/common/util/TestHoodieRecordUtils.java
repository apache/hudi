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

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.common.util.ConfigUtils.EMPTY_PROPS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestHoodieRecordUtils {

  @Test
  void loadHoodieMerge() {
    String mergeClassName = HoodieAvroRecordMerger.class.getName();
    HoodieRecordMerger recordMerger1 = HoodieRecordUtils.loadRecordMerger(mergeClassName);
    HoodieRecordMerger recordMerger2 = HoodieRecordUtils.loadRecordMerger(mergeClassName);
    assertEquals(recordMerger1.getClass().getName(), mergeClassName);
    assertEquals(recordMerger1, recordMerger2);
  }

  @Test
  void loadHoodieMergeWithWrongMerger() {
    String mergeClassName = "wrong.package.MergerName";
    assertThrows(HoodieException.class, () -> HoodieRecordUtils.loadRecordMerger(mergeClassName));
  }

  private static Iterable<Object[]> payloadClassNames() {
    List<Object[]> opts = new ArrayList<>();
    opts.add(new Object[] {DefaultHoodieRecordPayload.class.getName()});
    opts.add(new Object[] {DummyAvroPayload.class.getName()});
    return opts;
  }

  @ParameterizedTest
  @MethodSource("payloadClassNames")
  void testCreatePayload(String payloadClassName) throws IOException {
    HoodieRecordPayload payload = HoodieRecordUtils.createPayload(
        payloadClassName, null, 0, EMPTY_PROPS);
    assertEquals(payload.getClass().getName(), payloadClassName);

    GenericRecord record = new GenericData.Record(new Schema.Parser().parse(
        "{\"type\": \"record\"," + "\"name\": \"triprec\"," + "\"fields\": [ "
            + "{\"name\": \"timestamp\",\"type\": \"long\"} ]}"
    ));
    record.put("timestamp", 1L);
    payload = HoodieRecordUtils.createPayload(
        payloadClassName, record, EMPTY_PROPS);
    assertEquals(payload.getClass().getName(), payloadClassName);
  }

  public static class DummyAvroPayload extends OverwriteWithLatestAvroPayload {

    public DummyAvroPayload(GenericRecord gr, Comparable orderingVal) {
      super(gr, orderingVal);
    }

    public DummyAvroPayload(Option<GenericRecord> gr) {
      super(gr);
    }
  }
}