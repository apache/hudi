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

package org.apache.hudi.sink.utils;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.event.WriteMetadataEvent;

import lombok.Getter;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utilities for coordinator event buffer.
 */
public class EventBuffers implements Serializable {
  private static final long serialVersionUID = 1L;

  // {checkpointId -> (instant, data write events, index write events)}
  private final Map<Long, Pair<String, EventBuffer>> eventBuffers;
  private final Option<CommitGuard> commitGuardOption;
  private final int dataWriteParallelism;
  private final int indexWriteParallelism;

  private EventBuffers(
      Map<Long, Pair<String, EventBuffer>> eventBuffers,
      Option<CommitGuard> commitGuardOption,
      int dataWriteParallelism,
      int indexWriteParallelism) {
    this.eventBuffers = eventBuffers;
    this.commitGuardOption = commitGuardOption;
    this.dataWriteParallelism = dataWriteParallelism;
    this.indexWriteParallelism = indexWriteParallelism;
  }

  public static EventBuffers getInstance(Configuration conf, int dataWriteParallelism) {
    final Option<CommitGuard> commitGuardOpt = OptionsResolver.isBlockingInstantGeneration(conf)
        ? Option.of(CommitGuard.create(conf.get(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT))) : Option.empty();
    final int indexWriteParallelism = OptionsResolver.indexWriteParallelism(conf);
    return new EventBuffers(new ConcurrentSkipListMap<>(), commitGuardOpt, dataWriteParallelism, indexWriteParallelism);
  }

  public EventBuffer addEventToBuffer(WriteMetadataEvent event) {
    EventBuffer eventBuffer = this.eventBuffers.get(event.getCheckpointId()).getRight();
    eventBuffer.addEvent(event);
    return eventBuffer;
  }

  public void addEventsToBuffer(Map<Long, Pair<String, ?>> events) {
    events.forEach((ckpId, eventBufferPair) -> {
      if (eventBufferPair.getRight() instanceof EventBuffer) {
        this.eventBuffers.put(ckpId, (Pair<String, EventBuffer>) eventBufferPair);
      } else if (eventBufferPair.getRight() instanceof WriteMetadataEvent[]) {
        // This is for state compatibility handing when the checkpoint coordinator is recovered from the state of legacy format.
        EventBuffer eventBuffer = new EventBuffer((WriteMetadataEvent[]) eventBufferPair.getRight(), new WriteMetadataEvent[indexWriteParallelism]);
        this.eventBuffers.put(ckpId, Pair.of(eventBufferPair.getLeft(), eventBuffer));
      } else {
        throw new HoodieException("Unexpected event buffer type: " + eventBufferPair.getRight().getClass().getSimpleName());
      }
    });
  }

  /**
   * Returns existing bootstrap buffer or creates a new one.
   */
  public EventBuffer getOrCreateBootstrapBuffer(WriteMetadataEvent event) {
    return this.eventBuffers.computeIfAbsent(event.getCheckpointId(),
        ckpId -> Pair.of(event.getInstantTime(), new EventBuffer(dataWriteParallelism, indexWriteParallelism))).getRight();
  }

  /**
   * Cleans the task events triggered after or on the give event checkpoint ID.
   */
  public void cleanLegacyEvents(WriteMetadataEvent event) {
    this.eventBuffers.entrySet().stream()
        .filter(entry -> entry.getKey().compareTo(event.getCheckpointId()) >= 0)
        .map(entry -> entry.getValue().getRight())
        .forEach(eventBuffer -> eventBuffer.resetBuffer(event));
  }

  /**
   * Returns the pair of instant time and event buffer.
   */
  public Pair<String, EventBuffer> getInstantAndEventBuffer(long checkpointId) {
    return this.eventBuffers.get(checkpointId);
  }

  public EventBuffer getLatestEventBuffer(String instantTime) {
    return eventBuffers.values().stream().filter(val -> val.getLeft().equals(instantTime)).findFirst().map(Pair::getRight).orElse(null);
  }

  public EventBuffer getEventBuffer(long checkpointId) {
    return Option.ofNullable(this.eventBuffers.get(checkpointId)).map(Pair::getRight).orElse(null);
  }

  public Stream<Map.Entry<Long, Pair<String, EventBuffer>>> getEventBufferStream() {
    return this.eventBuffers.entrySet().stream();
  }

  public HashMap<Long, String> getAllCheckpointIdAndInstants() {
    HashMap<Long, String> result = new HashMap<>(eventBuffers.size());
    this.eventBuffers.forEach((k, v) -> result.put(k, v.getLeft()));
    return result;
  }

  public void initNewEventBuffer(long checkpointId, String instantTime) {
    this.eventBuffers.put(checkpointId, Pair.of(instantTime, new EventBuffer(dataWriteParallelism, indexWriteParallelism)));
  }

  public void awaitAllInstantsToCompleteIfNecessary() {
    if (this.commitGuardOption.isPresent() && nonEmpty()) {
      this.commitGuardOption.get().blockFor(getPendingInstants());
    }
  }

  public void reset(long checkpointId) {
    this.eventBuffers.remove(checkpointId);
    this.commitGuardOption.ifPresent(CommitGuard::unblock);
  }

  public boolean nonEmpty() {
    return this.eventBuffers.values().stream()
        .map(Pair::getValue)
        .flatMap(eventBuffer -> Stream.concat(Arrays.stream(eventBuffer.getDataWriteEventBuffer()), Arrays.stream(eventBuffer.getIndexWriteEventBuffer())))
        .anyMatch(Objects::nonNull);
  }

  public String getPendingInstants() {
    return this.eventBuffers.values().stream().map(Pair::getKey).collect(Collectors.joining(","));
  }

  /**
   * Get write metadata events where there exists no event sent by eager flushing from writers.
   */
  public Map<Long, Pair<String, EventBuffer>> getAllCompletedEvents() {
    return this.eventBuffers.entrySet().stream()
        .filter(entry -> entry.getValue().getRight().allEventsCompleted())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Buffers for writing metadata event for both data table and metadata table.
   */
  @Getter
  public static class EventBuffer implements Serializable {
    private static final long serialVersionUID = 1L;

    private final WriteMetadataEvent[] dataWriteEventBuffer;
    private final WriteMetadataEvent[] indexWriteEventBuffer;

    private EventBuffer(WriteMetadataEvent[] dataWriteEventBuffer, WriteMetadataEvent[] indexWriteEventBuffer) {
      this.dataWriteEventBuffer = dataWriteEventBuffer;
      this.indexWriteEventBuffer = indexWriteEventBuffer;
    }

    private EventBuffer(int dataWriteParallelism, int indexWriteParallelism) {
      this.dataWriteEventBuffer = new WriteMetadataEvent[dataWriteParallelism];
      this.indexWriteEventBuffer = new WriteMetadataEvent[indexWriteParallelism];
    }

    public void addEvent(WriteMetadataEvent event) {
      WriteMetadataEvent[] eventBuffer = event.isMetadataTable() ? indexWriteEventBuffer : dataWriteEventBuffer;
      if (eventBuffer[event.getTaskID()] != null
          && eventBuffer[event.getTaskID()].getInstantTime().equals(event.getInstantTime())) {
        eventBuffer[event.getTaskID()].mergeWith(event);
      } else {
        eventBuffer[event.getTaskID()] = event;
      }
    }

    public void addBootstrapEvent(WriteMetadataEvent event) {
      if (event.isMetadataTable()) {
        indexWriteEventBuffer[event.getTaskID()] = event;
      } else {
        dataWriteEventBuffer[event.getTaskID()] = event;
      }
    }

    /**
     * Checks the buffer is ready to commit.
     */
    public boolean allEventsReceived() {
      return Arrays.stream(dataWriteEventBuffer).allMatch(event -> event != null && event.isLastBatch())
          // all index write metadata events are received.
          && Arrays.stream(indexWriteEventBuffer).allMatch(event -> event != null && event.isLastBatch());
    }

    /**
     * Checks the buffer is ready to recommit on bootstrap.
     */
    public boolean allBootstrapEventsReceived() {
      return Arrays.stream(dataWriteEventBuffer).allMatch(event -> event != null && event.isBootstrap())
          // all index write metadata events are received.
          && Arrays.stream(indexWriteEventBuffer).allMatch(event -> event != null && event.isBootstrap());
    }

    /**
     * Reset write metadata event according to the task id in the event.
     */
    public void resetBuffer(WriteMetadataEvent event) {
      WriteMetadataEvent[] eventBuffer = event.isMetadataTable() ? indexWriteEventBuffer : dataWriteEventBuffer;
      if (eventBuffer.length > event.getTaskID()) {
        eventBuffer[event.getTaskID()] = null;
      }
    }

    /**
     * Return true if there is no event sent by eager flushing from writers.
     */
    public boolean allEventsCompleted() {
      return Stream.concat(Arrays.stream(dataWriteEventBuffer), Arrays.stream(indexWriteEventBuffer))
          .allMatch(event -> event == null || event.isLastBatch());
    }
  }
}
