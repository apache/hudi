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

import org.apache.hudi.hadoop.fs.RecordingLocalFileSystem;
import org.apache.hudi.hadoop.fs.RecordingLocalFileSystem.Call;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskFailedReason;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.SparkPlugin;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
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
 * folder, that they do not deserialize heavy driver-side objects with the task closure, and that
 * they do not parse the Hadoop default resources for every file they read.
 *
 * <p>The guards need the work under test to run as Spark tasks in this JVM, which a local session
 * does.
 */
public final class SparkExecutorGuards {

  /**
   * The attempt id of the Spark task that is running on, or that started, the current thread, set
   * by {@link TaskStartHookPlugin}. Threads a task creates inherit it.
   */
  private static final InheritableThreadLocal<Long> TASK_ATTEMPT = new InheritableThreadLocal<>();
  private static final Set<Long> RUNNING_TASK_ATTEMPTS = ConcurrentHashMap.newKeySet();

  /**
   * Whether the current thread runs a Spark task, or was started by a Spark task that is still
   * running (needs {@link TaskStartHookPlugin}). The thread name also covers the part of a task run
   * that precedes its {@link TaskContext}, namely deserializing the task and its partition.
   */
  public static final BooleanSupplier IN_SPARK_TASK = () -> {
    if (TaskContext.get() != null || Thread.currentThread().getName().startsWith("Executor task launch worker")) {
      return true;
    }
    Long startedBy = TASK_ATTEMPT.get();
    return startedBy != null && RUNNING_TASK_ATTEMPTS.contains(startedBy);
  };

  /**
   * The kind of deserialization a broadcast variable fetch is, which happens once per executor rather
   * than once per task.
   */
  public static final String BROADCAST_FETCH = "broadcast fetch";

  /**
   * The kind of deserialization the value of the file group reader's broadcast
   * {@code JavaSerializedValue} is: the scan state, deserialized once per JVM instance of the holder.
   */
  public static final String SCAN_STATE = "scan state";

  private static final String BROADCAST_CLASS = "org.apache.spark.broadcast.TorrentBroadcast";
  private static final String SCAN_STATE_HOLDER_CLASS = "org.apache.spark.sql.execution.datasources.parquet.JavaSerializedValue";

  /**
   * The {@code spark.plugins} value that enables {@link #setTaskStartHook}.
   */
  public static final String TASK_START_HOOK_PLUGIN = TaskStartHookPlugin.class.getName();

  private static final String SPARK_JOB_GROUP_ID = "spark.jobGroup.id";
  private static final String SPARK_JOB_DESCRIPTION = "spark.job.description";
  private static final String SPARK_JOB_INTERRUPT_ON_CANCEL = "spark.job.interruptOnCancel";
  private static final Pattern TASK_THREAD_STAGE = Pattern.compile("^Executor task launch worker .* in stage (\\d+)\\.");

  private static final int MAX_WRITTEN_BEFORE = 12;
  private static final int MAX_LOAD_SITES = 3;
  private static final int MAX_LOAD_SITE_FRAMES = 40;

  private static final URI LOCAL_FILE_SYSTEM = URI.create("file:///");

  private static volatile Runnable taskStartHook = () -> { };

  private SparkExecutorGuards() {
  }

  /**
   * Routes the {@code file} scheme through {@link RecordingLocalFileSystem} for every reader created
   * from {@code hadoopConf} (use the Spark context's Hadoop configuration), with Spark tasks as the
   * recording scope. Register it before writing the table too, so that all files are written and read
   * by the same file system implementation.
   *
   * <p>The recording file system also replaces the cached {@code file} file system, so that a reader
   * that resolves its file system from another configuration, such as a new one, is recorded too.
   */
  public static void enableFileSystemCallRecording(Configuration hadoopConf) {
    RecordingLocalFileSystem.setScope(IN_SPARK_TASK);
    RecordingLocalFileSystem.register(hadoopConf);
    try {
      FileSystem cached = FileSystem.get(LOCAL_FILE_SYSTEM, new Configuration());
      if (!(cached instanceof RecordingLocalFileSystem)) {
        // Closing removes it from the cache, so that the next lookup caches the recording file system.
        cached.close();
        Configuration recording = new Configuration();
        recording.set(RecordingLocalFileSystem.FILE_IMPL_KEY, RecordingLocalFileSystem.class.getName());
        FileSystem.get(LOCAL_FILE_SYSTEM, recording);
      }
      assertTrue(FileSystem.get(LOCAL_FILE_SYSTEM, new Configuration()) instanceof RecordingLocalFileSystem,
          "The cached file system of the file scheme must be the recording one");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Reverts {@link #enableFileSystemCallRecording}.
   */
  public static void disableFileSystemCallRecording(Configuration hadoopConf) {
    RecordingLocalFileSystem.unregister(hadoopConf);
    RecordingLocalFileSystem.setScope(() -> false);
    try {
      FileSystem cached = FileSystem.get(LOCAL_FILE_SYSTEM, new Configuration());
      if (cached instanceof RecordingLocalFileSystem) {
        cached.close();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Runs {@code action} and fails if any Spark task accessed a path under {@code .hoodie} while it
   * ran. Also fails if no Spark task call was recorded at all, which means the tasks do not use the
   * recording file system and the guard would pass vacuously.
   */
  public static <T> T assertNoExecutorMetaFolderAccess(String description, Supplier<T> action) {
    RecordingLocalFileSystem.reset();
    T result = action.get();
    assertTrue(RecordingLocalFileSystem.count(Call.inScope()) > 0,
        description + ": no file system call of a Spark task was recorded, so the guard is not live. Register "
            + RecordingLocalFileSystem.class.getSimpleName()
            + " on the Hadoop configuration the read uses (enableFileSystemCallRecording).");
    Predicate<Call> executorMetaFolderAccess = Call.inScope().and(Call.underMetaFolder());
    if (RecordingLocalFileSystem.count(executorMetaFolderAccess) > 0) {
      fail(description + ": Spark tasks accessed the .hoodie folder. Table config, timeline and schema"
          + " history must be resolved once on the driver and handed to the tasks; every access below"
          + " is repeated per task (per file group) on the executors.\n"
          + RecordingLocalFileSystem.describe(executorMetaFolderAccess));
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
    recorder.start(stageScope, SparkExecutorGuards::oncePerExecutorKind);
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

  private static String oncePerExecutorKind(String frameClass) {
    if (frameClass.startsWith(BROADCAST_CLASS)) {
      return BROADCAST_FETCH;
    }
    return frameClass.startsWith(SCAN_STATE_HOLDER_CLASS) ? SCAN_STATE : null;
  }

  /**
   * Sets code to run on the task thread at the start of every Spark task, after the task and its
   * partition are deserialized and before the task runs. It takes effect only in a session whose {@code spark.plugins} includes
   * {@link TaskStartHookPlugin}, see {@link #TASK_START_HOOK_PLUGIN}.
   */
  public static void setTaskStartHook(Runnable hook) {
    taskStartHook = hook;
  }

  /**
   * A Spark plugin that runs the hook set by {@link #setTaskStartHook} when a task starts and tracks
   * the running tasks for the threads they start, see {@link #IN_SPARK_TASK}.
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
          TaskContext context = TaskContext.get();
          if (context != null) {
            RUNNING_TASK_ATTEMPTS.add(context.taskAttemptId());
            TASK_ATTEMPT.set(context.taskAttemptId());
          }
          taskStartHook.run();
        }

        @Override
        public void onTaskSucceeded() {
          taskEnded();
        }

        @Override
        public void onTaskFailed(TaskFailedReason failureReason) {
          taskEnded();
        }

        // Spark may unset the task context before these callbacks, so the attempt comes from the thread.
        private void taskEnded() {
          Long attempt = TASK_ATTEMPT.get();
          if (attempt != null) {
            RUNNING_TASK_ATTEMPTS.remove(attempt);
          }
          TASK_ATTEMPT.remove();
        }
      };
    }
  }

  /**
   * Runs {@code action} and fails if Spark tasks parsed the Hadoop default resources more than
   * {@code maxLoads} times, which every {@link Configuration} created with defaults does on its first
   * read. A task must reuse the configuration it was given instead, since the parse is repeated for
   * every configuration created this way, typically once per file read.
   *
   * <p>Each task's context class loader is wrapped, once the task starts running, to count the lookups
   * of {@code core-default.xml}. A new configuration takes the context class loader of the thread that
   * creates it, so the count covers configurations the task creates while it runs; a copy keeps the
   * class loader of the configuration it copies. Needs a session with {@link #TASK_START_HOOK_PLUGIN}.
   * After {@code action}, a single task that creates a configuration checks that the count is live.
   */
  public static <T> T assertTaskHadoopDefaultResourceLoadsAtMost(String description, SparkContext sparkContext,
                                                                 int maxLoads, Supplier<T> action) {
    AtomicInteger tasks = new AtomicInteger();
    AtomicInteger loads = new AtomicInteger();
    Queue<Throwable> loadSites = new ConcurrentLinkedQueue<>();
    T result;
    setTaskStartHook(() -> {
      tasks.incrementAndGet();
      Thread thread = Thread.currentThread();
      thread.setContextClassLoader(
          new DefaultResourceCountingClassLoader(thread.getContextClassLoader(), loads, loadSites));
    });
    try {
      result = action.get();
      int actionTasks = tasks.get();
      int actionLoads = loads.get();
      List<Throwable> actionLoadSites = new ArrayList<>(loadSites);
      JavaSparkContext.fromSparkContext(sparkContext).parallelize(Collections.singletonList(1), 1)
          .foreach(i -> new Configuration().get("fs.defaultFS"));
      assertTrue(actionTasks > 0 && loads.get() > actionLoads,
          description + ": the Hadoop default resource count is not live (tasks of the action: " + actionTasks
              + "); run the session with " + TASK_START_HOOK_PLUGIN + " in spark.plugins");
      if (actionLoads > maxLoads) {
        fail(description + ": Spark tasks parsed the Hadoop default resources " + actionLoads + " times in "
            + actionTasks + " tasks, more than " + maxLoads + ". A reader must use the configuration of the task,"
            + " not create one per file, which parses core-default.xml and core-site.xml each time. First parses:\n"
            + actionLoadSites.stream().map(SparkExecutorGuards::describeLoadSite)
                .collect(Collectors.joining("\n")));
      }
    } finally {
      setTaskStartHook(() -> { });
    }
    return result;
  }

  private static String describeLoadSite(Throwable site) {
    return Arrays.stream(site.getStackTrace())
        .skip(1)
        .limit(MAX_LOAD_SITE_FRAMES)
        .map(frame -> "    at " + frame)
        .collect(Collectors.joining("\n"));
  }

  /**
   * Counts lookups of {@code core-default.xml}, which a {@link Configuration} makes once per parse of
   * its default resources, and keeps the stack of the first few.
   */
  private static final class DefaultResourceCountingClassLoader extends ClassLoader {
    private final AtomicInteger loads;
    private final Queue<Throwable> loadSites;

    private DefaultResourceCountingClassLoader(ClassLoader parent, AtomicInteger loads, Queue<Throwable> loadSites) {
      super(parent);
      this.loads = loads;
      this.loadSites = loadSites;
    }

    @Override
    public URL getResource(String name) {
      if ("core-default.xml".equals(name) && loads.incrementAndGet() <= MAX_LOAD_SITES) {
        loadSites.add(new Throwable());
      }
      return super.getResource(name);
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
   * Fails if the task binary holds, or tasks deserialized outside once per executor deserializations,
   * an instance of any class that is or extends one of {@code forbiddenClassNames}, if the scan state
   * holds an instance of any class that is or extends one of {@code forbiddenInScanState}, if the task
   * binary is larger than {@code maxTaskBinaryBytes}, or if a task stream larger than
   * {@code maxTaskStreamBytes} was deserialized. Also fails if no Spark RDD was recorded, which means
   * the recorder did not see the task binary.
   */
  public static void assertTaskDeserializationFootprint(String description,
                                                        TaskDeserializationRecorder.Result result,
                                                        TaskBinary taskBinary,
                                                        Collection<String> forbiddenClassNames,
                                                        Collection<String> forbiddenInScanState,
                                                        long maxTaskBinaryBytes,
                                                        long maxTaskStreamBytes) {
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
    for (String forbidden : forbiddenInScanState) {
      List<String> inScanState = result.exemptNamesOfSubtypesOf(SCAN_STATE, forbidden);
      if (!inScanState.isEmpty()) {
        violations.add("in the broadcast scan state: " + inScanState + " (forbidden: " + forbidden + ")");
      }
    }
    if (taskBinary.getBytes() > maxTaskBinaryBytes) {
      violations.add("the task binary is " + taskBinary.getBytes() + " bytes, over the budget of " + maxTaskBinaryBytes + " bytes");
    }
    if (result.getMaxStreamBytes() > maxTaskStreamBytes) {
      violations.add("a task stream of " + result.getMaxStreamBytes() + " bytes was deserialized, over the budget of "
          + maxTaskStreamBytes + " bytes");
    }
    if (!violations.isEmpty()) {
      fail(description + ": Spark tasks deserialize heavy driver-side state or more bytes than budgeted with"
          + " every task; it is paid again by every task. Move it to a broadcast or out of the closure.\n  "
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
