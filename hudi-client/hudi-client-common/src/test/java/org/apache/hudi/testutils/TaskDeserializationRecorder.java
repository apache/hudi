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

package org.apache.hudi.testutils;

import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Records what engine tasks Java-deserialize, through a JVM-wide {@link ObjectInputFilter}.
 *
 * <p>An engine ships each task its closure (for Spark, the task binary holding the RDD and the
 * function to run) as a Java-serialized stream that every task deserializes again. Whatever a
 * closure captures therefore costs deserialization CPU and memory per task, which is why heavy
 * driver-side objects (a table meta client, its timeline, a full Hadoop configuration) must not be
 * captured. A functional test uses this recorder to assert on the classes tasks deserialize and on
 * the size of the largest task stream.
 *
 * <p>Streams read under an exempt stack frame (for Spark, a broadcast variable fetch, which is
 * deserialized once per executor rather than once per task) are recorded separately and do not
 * count towards {@link Result#getMaxStreamBytes()}.
 *
 * <p>The filter never rejects anything: it always answers {@link ObjectInputFilter.Status#UNDECIDED},
 * so installing it does not change what deserializes. It is installed once per JVM and stays
 * installed; outside {@link #start}/{@link #stop} it only reads a volatile field.
 */
public final class TaskDeserializationRecorder implements ObjectInputFilter {

  private static final TaskDeserializationRecorder INSTANCE = new TaskDeserializationRecorder();
  private static final int MAX_STACK_FRAMES = 30;
  private static final Set<String> CLASS_DESCRIPTOR_METHODS = new HashSet<>(Arrays.asList(
      "filterCheck", "readClassDesc", "readNonProxyDesc", "readProxyDesc", "readClassDescriptor"));

  private volatile Session session;

  private TaskDeserializationRecorder() {
  }

  /**
   * Installs the recorder as the JVM-wide serialization filter if it is not installed yet.
   *
   * @throws IllegalStateException if another JVM-wide filter is already installed; the JDK accepts
   *                               only one per JVM, so this recorder cannot be chained after it
   */
  public static synchronized TaskDeserializationRecorder install() {
    ObjectInputFilter current = ObjectInputFilter.Config.getSerialFilter();
    if (current == INSTANCE) {
      return INSTANCE;
    }
    if (current != null) {
      throw new IllegalStateException("A JVM-wide serialization filter is already installed (" + current
          + ", e.g. from -Djdk.serialFilter), and the JDK allows only one per JVM, so "
          + TaskDeserializationRecorder.class.getSimpleName() + " cannot record task deserialization. "
          + "Run this test without that filter.");
    }
    ObjectInputFilter.Config.setSerialFilter(INSTANCE);
    return INSTANCE;
  }

  /**
   * Starts recording, keeping what each task scope deserializes apart so that {@link #stop(Predicate)}
   * can keep only the scopes of the work under test, for example the stages of one job.
   *
   * @param taskScope     the scope of the task the current thread runs, or null outside tasks
   * @param exemptFrameKind the kind of deserialization that is not per task, such as a broadcast
   *                      variable fetch, that a stack frame of the given class marks, or null
   */
  public synchronized void start(Supplier<String> taskScope, Function<String, String> exemptFrameKind) {
    if (session != null) {
      throw new IllegalStateException("A recording is already in progress");
    }
    session = new Session(taskScope, exemptFrameKind);
  }

  /**
   * Stops recording and returns what the task scopes accepted by {@code keepScope} deserialized
   * since {@link #start}.
   */
  public synchronized Result stop(Predicate<String> keepScope) {
    Session current = session;
    if (current == null) {
      throw new IllegalStateException("No recording in progress");
    }
    session = null;
    return new Result(current, keepScope);
  }

  @Override
  public Status checkInput(FilterInfo filterInfo) {
    Session current = session;
    if (current == null) {
      return Status.UNDECIDED;
    }
    try {
      String scope = current.taskScope.get();
      if (scope != null) {
        ScopeRecord record = current.scopes.computeIfAbsent(scope, s -> new ScopeRecord());
        Class<?> clazz = filterInfo.serialClass();
        String exemptKind = StackWalker.getInstance().walk(frames -> frames
            .map(frame -> current.exemptFrameKind.apply(frame.getClassName()))
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null));
        // A Class object in the stream, such as the runtime class of a ClassTag, is not an instance.
        boolean instance = clazz != null && !isClassReference();
        if (exemptKind != null) {
          // A stream reports a class once, so this counts the exempt deserializations reading the class.
          if (instance) {
            record.exemptDeserializations.computeIfAbsent(exemptKind, kind -> new ConcurrentHashMap<>())
                .computeIfAbsent(clazz, c -> new AtomicLong()).incrementAndGet();
          }
        } else {
          if (instance && record.classes.add(clazz)) {
            record.firstStacks.putIfAbsent(clazz, stackOfCaller());
          }
          record.maxStreamBytes.accumulateAndGet(filterInfo.streamBytes(), Math::max);
        }
      }
    } catch (Throwable t) {
      // Recording must never interfere with deserialization.
      current.errors.incrementAndGet();
    }
    return Status.UNDECIDED;
  }

  /**
   * Whether the class descriptor being checked belongs to a {@code Class} object in the stream, for
   * example the runtime class of a {@code ClassTag}, rather than to an instance being deserialized.
   */
  private static boolean isClassReference() {
    return StackWalker.getInstance().walk(frames -> frames
        .filter(frame -> frame.getClassName().equals(ObjectInputStream.class.getName()))
        .map(StackWalker.StackFrame::getMethodName)
        .filter(method -> !CLASS_DESCRIPTOR_METHODS.contains(method))
        .findFirst()
        .map("readClass"::equals)
        .orElse(false));
  }

  private static String stackOfCaller() {
    return StackWalker.getInstance().walk(frames -> frames
        .skip(1)
        .filter(frame -> !frame.getClassName().startsWith("java.io.ObjectInputStream")
            && !frame.getClassName().equals(TaskDeserializationRecorder.class.getName()))
        .limit(MAX_STACK_FRAMES)
        .map(frame -> "\tat " + frame)
        .collect(Collectors.joining("\n")));
  }

  /**
   * Whether {@code clazz} is the class named {@code className} or extends or implements it.
   */
  public static boolean isSubtypeOf(Class<?> clazz, String className) {
    if (clazz == null) {
      return false;
    }
    if (clazz.getName().equals(className) || isSubtypeOf(clazz.getSuperclass(), className)) {
      return true;
    }
    for (Class<?> iface : clazz.getInterfaces()) {
      if (isSubtypeOf(iface, className)) {
        return true;
      }
    }
    return false;
  }

  private static final class Session {
    private final Supplier<String> taskScope;
    private final Function<String, String> exemptFrameKind;
    private final Map<String, ScopeRecord> scopes = new ConcurrentHashMap<>();
    private final AtomicLong errors = new AtomicLong();

    private Session(Supplier<String> taskScope, Function<String, String> exemptFrameKind) {
      this.taskScope = taskScope;
      this.exemptFrameKind = exemptFrameKind;
    }
  }

  private static final class ScopeRecord {
    private final Set<Class<?>> classes = ConcurrentHashMap.newKeySet();
    private final Map<Class<?>, String> firstStacks = new ConcurrentHashMap<>();
    private final Map<String, Map<Class<?>, AtomicLong>> exemptDeserializations = new ConcurrentHashMap<>();
    private final AtomicLong maxStreamBytes = new AtomicLong();
  }

  /**
   * What tasks deserialized during one recording.
   */
  public static final class Result {
    private final Set<Class<?>> classes;
    private final Map<Class<?>, String> firstStacks;
    private final Map<String, Map<Class<?>, Long>> exemptDeserializations;
    private final Set<String> keptScopes;
    private final Set<String> ignoredScopes;
    private final long maxStreamBytes;
    private final long errors;

    private Result(Session session, Predicate<String> keepScope) {
      Set<Class<?>> keptClasses = new HashSet<>();
      Map<Class<?>, String> keptFirstStacks = new HashMap<>();
      Map<String, Map<Class<?>, Long>> keptExemptDeserializations = new HashMap<>();
      Set<String> kept = new TreeSet<>();
      Set<String> ignored = new TreeSet<>();
      long maxBytes = 0;
      for (Map.Entry<String, ScopeRecord> entry : session.scopes.entrySet()) {
        if (keepScope.test(entry.getKey())) {
          kept.add(entry.getKey());
          keptClasses.addAll(entry.getValue().classes);
          entry.getValue().firstStacks.forEach(keptFirstStacks::putIfAbsent);
          entry.getValue().exemptDeserializations.forEach((kind, counts) -> counts.forEach((clazz, count) ->
              keptExemptDeserializations.computeIfAbsent(kind, k -> new HashMap<>()).merge(clazz, count.get(), Long::sum)));
          maxBytes = Math.max(maxBytes, entry.getValue().maxStreamBytes.get());
        } else {
          ignored.add(entry.getKey());
        }
      }
      this.classes = Collections.unmodifiableSet(keptClasses);
      this.firstStacks = Collections.unmodifiableMap(keptFirstStacks);
      this.exemptDeserializations = Collections.unmodifiableMap(keptExemptDeserializations);
      this.keptScopes = Collections.unmodifiableSet(kept);
      this.ignoredScopes = Collections.unmodifiableSet(ignored);
      this.maxStreamBytes = maxBytes;
      this.errors = session.errors.get();
    }

    /**
     * How many deserializations of the given exempt kind read an instance of the class named
     * {@code className}.
     */
    public long getExemptDeserializations(String kind, String className) {
      return exemptDeserializations.getOrDefault(kind, Collections.emptyMap()).entrySet().stream()
          .filter(entry -> entry.getKey().getName().equals(className))
          .mapToLong(Map.Entry::getValue)
          .sum();
    }

    /**
     * Names of the classes that are or extend {@code className} and that deserializations of the
     * given exempt kind read an instance of.
     */
    public List<String> exemptNamesOfSubtypesOf(String kind, String className) {
      return exemptDeserializations.getOrDefault(kind, Collections.emptyMap()).keySet().stream()
          .filter(clazz -> isSubtypeOf(clazz, className))
          .map(Class::getName)
          .sorted()
          .collect(Collectors.toList());
    }

    /**
     * Where a task first deserialized the class named {@code className} outside exempt frames, or
     * null if no task did.
     */
    public String getFirstStack(String className) {
      return firstStacks.entrySet().stream()
          .filter(entry -> entry.getKey().getName().equals(className))
          .map(Map.Entry::getValue)
          .findFirst().orElse(null);
    }

    /**
     * The task scopes whose deserialization this result holds.
     */
    public Set<String> getKeptScopes() {
      return keptScopes;
    }

    /**
     * The task scopes recorded but left out of this result.
     */
    public Set<String> getIgnoredScopes() {
      return ignoredScopes;
    }

    /**
     * Classes deserialized by tasks outside exempt frames.
     */
    public Set<Class<?>> getClasses() {
      return classes;
    }

    /**
     * The largest byte count any task stream outside exempt frames had consumed when the filter
     * was consulted, a close lower bound of the largest per-task stream.
     */
    public long getMaxStreamBytes() {
      return maxStreamBytes;
    }

    /**
     * How many filter callbacks failed to record; non-zero means the result is incomplete.
     */
    public long getErrors() {
      return errors;
    }

    /**
     * Names of the classes deserialized outside exempt frames that are {@code className} or extend
     * or implement it. Matching by name lets a caller check for classes it cannot compile against.
     */
    public List<String> namesOfSubtypesOf(String className) {
      return classes.stream()
          .filter(clazz -> isSubtypeOf(clazz, className))
          .map(Class::getName)
          .sorted()
          .collect(Collectors.toList());
    }
  }
}
