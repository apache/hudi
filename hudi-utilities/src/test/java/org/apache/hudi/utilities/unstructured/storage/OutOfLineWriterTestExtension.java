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

package org.apache.hudi.utilities.unstructured.storage;

import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.marker.TimelineServerBasedWriteMarkers;
import org.apache.hudi.table.marker.WriteMarkers;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.MockedStatic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/** Keeps direct prep unit tests independent of a running streamer while exercising the real shared writer and storage. */
public class OutOfLineWriterTestExtension implements BeforeEachCallback, AfterEachCallback {
  private MockedStatic<OutOfLineWriter> factory;

  @Override
  public void beforeEach(ExtensionContext context) {
    factory = mockStatic(OutOfLineWriter.class, CALLS_REAL_METHODS);
    factory.when(() -> OutOfLineWriter.forTable(any(), anyString(), any()))
        .thenAnswer(invocation -> new MarkingFactory(invocation.getArgument(1)));
  }

  /** Needs no table: each writer's markers accept every CREATE marker. */
  private static final class MarkingFactory extends OutOfLineWriter.Factory {
    MarkingFactory(String basePath) {
      super(basePath, null);
    }

    @Override
    Option<WriteMarkers> markers(String instant) {
      TimelineServerBasedWriteMarkers markers = mock(TimelineServerBasedWriteMarkers.class);
      when(markers.create(anyString(), anyString(), eq(IOType.CREATE)))
          .thenAnswer(marker -> Option.of(new StoragePath(marker.getArgument(0) + "/" + marker.getArgument(1))));
      return Option.of(markers);
    }
  }

  @Override
  public void afterEach(ExtensionContext context) {
    factory.close();
  }
}
