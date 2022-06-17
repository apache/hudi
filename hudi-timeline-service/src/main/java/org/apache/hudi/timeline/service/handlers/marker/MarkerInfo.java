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

package org.apache.hudi.timeline.service.handlers.marker;

import org.apache.commons.io.input.Tailer;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

public class MarkerInfo {

  private String instant;
  // marker file -> makers
  private ConcurrentHashMap<String, CopyOnWriteArraySet<String>> markerFile2markers;
  // marker file -> tailers
  private ConcurrentHashMap<String, Tailer> markerFile2Tailer;


  public MarkerInfo(String instant, ConcurrentHashMap<String, CopyOnWriteArraySet<String>> markerFile2markers,
                    ConcurrentHashMap<String, Tailer> markerFile2Tailer) {
    this.instant = instant;
    this.markerFile2markers = markerFile2markers;
    this.markerFile2Tailer = markerFile2Tailer;
  }

  public void close() {
    markerFile2Tailer.values().forEach(Tailer::stop);
  }

  public HashSet<String> getAllMarkerFiles() {
    return new HashSet<>(markerFile2Tailer.keySet());
  }

  public void add(String markerFileName, CopyOnWriteArraySet<String> markers, Tailer tailer) {
    markerFile2markers.put(markerFileName, markers);
    markerFile2Tailer.put(markerFileName, tailer);
  }

  public Set<String> getAllMarkers() {
    return markerFile2markers.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());

  }
}
