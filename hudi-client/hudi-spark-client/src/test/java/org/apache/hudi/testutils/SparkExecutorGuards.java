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

import org.apache.hudi.hadoop.fs.MetaFolderAccessRecordingFileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.SparkPlugin;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import scala.reflect.ClassTag$;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Guards for what Spark tasks do on executors: that they do not touch a table's {@code .hoodie}
 * folder, and that they do not deserialize heavy driver-side objects with the task closure.
 *
 * <p>Both guards need the work under test to run as Spark tasks in this JVM, which a
 * {@code local[*]} session does.
 */
public final class SparkExecutorGuards {

  /**
   * Whether the current thread runs a Spark task. The thread name also covers the part of a task
   * run that precedes its {@link TaskContext}, namely deserializing the task and its partition.
   */
  public static final BooleanSupplier IN_SPARK_TASK = () -> TaskContext.get() != null
      || Thread.currentThread().getName().startsWith("Executor task launch worker");

  /**
   * Stack frames of this class mark a broadcast variable fetch, which is deserialized once per
   * executor rather than once per task.
   */
  private static final String BROADCAST_CLASS = "org.apache.spark.broadcast.TorrentBroadcast";

  /**
   * The {@code spark.plugins} value that enables {@link #setTaskStartHook}.
   */
  public static final String TASK_START_HOOK_PLUGIN = TaskStartHookPlugin.class.getName();

  private static final String SPARK_JOB_GROUP_ID = "spark.jobGroup.id";
  private static final String SPARK_JOB_DESCRIPTION = "spark.job.description";
  private static final String SPARK_JOB_INTERRUPT_ON_CANCEL = "spark.job.interruptOnCancel";
  private static final Pattern TASK_THREAD_STAGE = Pattern.compile("^Executor task launch worker .* in stage (\\d+)\\.");

  private static final int MAX_WRITTEN_BEFORE = 12;
  private static final String[] FILE_SCHEME_KEYS = {"fs.file.impl", "fs.file.impl.disable.cache"};
  private static final Map<Configuration, Map<String, String>> VALUES_BEFORE_RECORDING = new WeakHashMap<>();

  private static volatile Runnable taskStartHook = () -> { };

  private SparkExecutorGuards() {
  }

  /**
   * Routes the {@code file} scheme through {@link MetaFolderAccessRecordingFileSystem} for every
   * reader created from {@code hadoopConf} (use the Spark context's Hadoop configuration). Register
   * it before writing the table too, so that all files are written and read by the same file
   * system implementation.
   */
  public static void enableMetaFolderAccessRecording(Configuration hadoopConf) {
    synchronized (VALUES_BEFORE_RECORDING) {
      VALUES_BEFORE_RECORDING.computeIfAbsent(hadoopConf, conf -> {
        Map<String, String> values = new HashMap<>();
        for (String key : FILE_SCHEME_KEYS) {
          values.put(key, conf.getRaw(key));
        }
        return values;
      });
    }
    MetaFolderAccessRecordingFileSystem.setTaskScope(IN_SPARK_TASK);
    MetaFolderAccessRecordingFileSystem.register(hadoopConf);
  }

  /**
   * Reverts {@link #enableMetaFolderAccessRecording}, restoring the {@code file} scheme settings the
   * configuration had before.
   */
  public static void disableMetaFolderAccessRecording(Configuration hadoopConf) {
    MetaFolderAccessRecordingFileSystem.unregister(hadoopConf);
    Map<String, String> values;
    synchronized (VALUES_BEFORE_RECORDING) {
      values = VALUES_BEFORE_RECORDING.remove(hadoopConf);
    }
    if (values != null) {
      values.forEach((key, value) -> {
        if (value != null) {
          hadoopConf.set(key, value);
        }
      });
    }
  }

  /**
   * Runs {@code action} and fails if any Spark task accessed a path under {@code .hoodie} while it
   * ran. Also fails if the driver did not access {@code .hoodie} either, which means the recording
   * file system is not in use and the guard would pass vacuously.
   */
  public static <T> T assertNoExecutorMetaFolderAccess(String description, Supplier<T> action) {
    MetaFolderAccessRecordingFileSystem.reset();
    T result = action.get();
    assertTrue(MetaFolderAccessRecordingFileSystem.getNonTaskAccessCount() > 0,
        description + ": no .hoodie access was recorded at all, so the guard is not live. Register "
            + MetaFolderAccessRecordingFileSystem.class.getSimpleName()
            + " on the Hadoop configuration the read uses (enableMetaFolderAccessRecording).");
    if (!MetaFolderAccessRecordingFileSystem.getTaskAccesses().isEmpty()) {
      fail(description + ": Spark tasks accessed the .hoodie folder. Table config, timeline and schema"
          + " history must be resolved once on the driver and handed to the tasks; every access below"
          + " is repeated per task (per file group) on the executors.\n"
          + MetaFolderAccessRecordingFileSystem.describeTaskAccesses());
    }
    return result;
  }

  /**
   * Runs {@code action} while recording the classes Spark tasks Java-deserialize, keeping only the
   * tasks of the jobs {@code action} starts. Tasks of jobs that run concurrently, such as file
   * listing jobs started by planning in another thread, are left out.
   */
  public static TaskDeserializationRecorder.Result recordTaskDeserialization(SparkContext sparkContext, Runnable action) {
    String jobGroup = "task-deserialization-" + UUID.randomUUID();
    Set<String> scopesOfJobGroup = ConcurrentHashMap.newKeySet();
    Supplier<String> stageScope = () -> {
      TaskContext taskContext = TaskContext.get();
      if (taskContext != null) {
        String scope = "stage " + taskContext.stageId();
        if (jobGroup.equals(taskContext.getLocalProperty(SPARK_JOB_GROUP_ID))) {
          scopesOfJobGroup.add(scope);
        }
        return scope;
      }
      Matcher matcher = TASK_THREAD_STAGE.matcher(Thread.currentThread().getName());
      return matcher.find() ? "stage " + matcher.group(1) : null;
    };
    TaskDeserializationRecorder recorder = TaskDeserializationRecorder.install();
    String previousJobGroup = sparkContext.getLocalProperty(SPARK_JOB_GROUP_ID);
    String previousDescription = sparkContext.getLocalProperty(SPARK_JOB_DESCRIPTION);
    String previousInterrupt = sparkContext.getLocalProperty(SPARK_JOB_INTERRUPT_ON_CANCEL);
    sparkContext.setJobGroup(jobGroup, "task deserialization recording", false);
    recorder.start(stageScope, frameClass -> frameClass.startsWith(BROADCAST_CLASS));
    TaskDeserializationRecorder.Result result;
    try {
      action.run();
    } finally {
      result = recorder.stop(scopesOfJobGroup::contains);
      sparkContext.setLocalProperty(SPARK_JOB_GROUP_ID, previousJobGroup);
      sparkContext.setLocalProperty(SPARK_JOB_DESCRIPTION, previousDescription);
      sparkContext.setLocalProperty(SPARK_JOB_INTERRUPT_ON_CANCEL, previousInterrupt);
    }
    return result;
  }

  /**
   * Sets code to run on the task thread at the start of every Spark task, before the task reads
   * anything. It takes effect only in a session whose {@code spark.plugins} includes
   * {@link TaskStartHookPlugin}, see {@link #TASK_START_HOOK_PLUGIN}.
   */
  public static void setTaskStartHook(Runnable hook) {
    taskStartHook = hook;
  }

  /**
   * A Spark plugin that runs the hook set by {@link #setTaskStartHook} when a task starts.
   */
  public static final class TaskStartHookPlugin implements SparkPlugin {
    @Override
    public DriverPlugin driverPlugin() {
      return null;
    }

    @Override
    public ExecutorPlugin executorPlugin() {
      return new ExecutorPlugin() {
        @Override
        public void onTaskStart() {
          taskStartHook.run();
        }
      };
    }
  }

  /**
   * The task binary Spark ships to every task that reads {@code df}: the RDD lineage of its physical
   * plan, Java-serialized as the DAG scheduler does. Call it after the plan ran; it does not run a job.
   */
  public static TaskBinary inspectTaskBinary(Dataset<?> df) {
    RDD<?> rdd = df.queryExecution().executedPlan().execute();
    long bytes = SparkEnv.get().closureSerializer().newInstance()
        .serialize(rdd, ClassTag$.MODULE$.apply(RDD.class)).remaining();
    Map<Class<?>, List<String>> instances = new LinkedHashMap<>();
    Deque<String> recentlyWritten = new ArrayDeque<>();
    try (ObjectOutputStream out = new ObjectOutputStream(OutputStream.nullOutputStream()) {
      {
        enableReplaceObject(true);
      }

      @Override
      protected Object replaceObject(Object obj) {
        instances.computeIfAbsent(obj.getClass(), c -> new ArrayList<>(recentlyWritten));
        recentlyWritten.addLast(obj.getClass().getName());
        if (recentlyWritten.size() > MAX_WRITTEN_BEFORE) {
          recentlyWritten.removeFirst();
        }
        return obj;
      }
    }) {
      out.writeObject(rdd);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return new TaskBinary(bytes, instances);
  }

  /**
   * The size of the task binary and the classes of the instances it holds.
   */
  public static final class TaskBinary {
    private final long bytes;
    private final Map<Class<?>, List<String>> instances;

    private TaskBinary(long bytes, Map<Class<?>, List<String>> instances) {
      this.bytes = bytes;
      this.instances = instances;
    }

    public long getBytes() {
      return bytes;
    }

    /**
     * Names of the classes of instances in the task binary that are {@code className} or extend or
     * implement it.
     */
    public List<String> namesOfSubtypesOf(String className) {
      return instances.keySet().stream()
          .filter(clazz -> TaskDeserializationRecorder.isSubtypeOf(clazz, className))
          .map(Class::getName)
          .sorted()
          .collect(Collectors.toList());
    }

    /**
     * The classes of the objects serialized right before the first instance of {@code className},
     * which include the objects that hold it.
     */
    public List<String> writtenBefore(String className) {
      return instances.entrySet().stream()
          .filter(entry -> entry.getKey().getName().equals(className))
          .map(Map.Entry::getValue)
          .findFirst().orElse(Collections.emptyList());
    }
  }

  /**
   * Fails if the task binary holds, or tasks deserialized outside broadcast fetches, an instance of
   * any class that is or extends one of {@code forbiddenClassNames}, or if the task binary is larger
   * than {@code maxTaskBinaryBytes}. Also fails if no Spark RDD was recorded, which means the
   * recorder did not see the task binary.
   */
  public static void assertTaskDeserializationFootprint(String description,
                                                        TaskDeserializationRecorder.Result result,
                                                        TaskBinary taskBinary,
                                                        Collection<String> forbiddenClassNames,
                                                        long maxTaskBinaryBytes) {
    assertEquals(0, result.getErrors(), description + ": the deserialization recorder failed on some callbacks");
    assertTrue(!result.namesOfSubtypesOf("org.apache.spark.rdd.RDD").isEmpty(),
        description + ": no RDD was deserialized on a task thread, so the recorder did not see the task binary."
            + " Recorded classes: " + sortedNames(result));
    assertTrue(!taskBinary.namesOfSubtypesOf("org.apache.spark.rdd.RDD").isEmpty(),
        description + ": the task binary inspection saw no RDD instance, so it did not see the task binary");
    List<String> violations = new ArrayList<>();
    for (String forbidden : forbiddenClassNames) {
      List<String> inBinary = taskBinary.namesOfSubtypesOf(forbidden);
      if (!inBinary.isEmpty()) {
        violations.add("in the task binary: " + inBinary + " (forbidden: " + forbidden + "), written right after "
            + taskBinary.writtenBefore(inBinary.get(0)));
      }
      List<String> deserialized = result.namesOfSubtypesOf(forbidden);
      if (!deserialized.isEmpty()) {
        violations.add("deserialized per task: " + deserialized + " (forbidden: " + forbidden + "), first at:\n"
            + result.getFirstStack(deserialized.get(0)));
      }
    }
    if (taskBinary.getBytes() > maxTaskBinaryBytes) {
      violations.add("the task binary is " + taskBinary.getBytes() + " bytes, over the budget of " + maxTaskBinaryBytes + " bytes");
    }
    if (!violations.isEmpty()) {
      fail(description + ": Spark tasks deserialize heavy driver-side state with the task closure; it is"
          + " paid again by every task. Move it to a broadcast or out of the closure.\n  "
          + String.join("\n  ", violations)
          + "\nTask binary: " + taskBinary.getBytes() + " bytes (largest task stream seen by the filter: "
          + result.getMaxStreamBytes() + " bytes)"
          + "\nHudi classes deserialized per task: " + sortedNames(result).stream()
              .filter(name -> name.startsWith("org.apache.hudi") || name.contains(".hudi."))
              .collect(Collectors.toList()));
    }
  }

  private static List<String> sortedNames(TaskDeserializationRecorder.Result result) {
    return result.getClasses().stream().map(Class::getName).sorted().collect(Collectors.toList());
  }
}
