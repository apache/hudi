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

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.junit.platform.engine.FilterResult;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.PostDiscoveryFilter;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

/**
 * Runs one scenario class (or some of its methods) with the JUnit platform launcher in a CHILD JVM whose class path is this JVM's
 * test class path without FFmpeg ({@link NoFfmpegClassPath}), and reports each test's outcome. The child is the running JVM's own
 * java binary with its JVM options (heap, add-opens) and the system properties a Spark test reads; its output goes to a log file.
 * {@link #main} is the child's entry point.
 */
public final class ScenarioJvm {

  /** How long the parent waits for the child before killing it. */
  public static final long TIMEOUT_MINUTES = 10;
  /** Lines of the child's log in a failure message. */
  private static final int LOG_TAIL_LINES = 120;
  /** Exit code of a child whose filter selected no test. */
  static final int EXIT_NO_TESTS = 2;
  /** Exit code of a child whose run had a failure. */
  static final int EXIT_FAILED = 1;

  /** System properties passed to the child as they are set in this JVM. */
  private static final List<String> PASSED_PROPERTIES = Collections.unmodifiableList(Arrays.asList(
      "java.io.tmpdir", "log4j.configurationFile", "log4j2.configurationFile", "user.timezone", "file.encoding", "basedir"));
  /** Prefixes of further system properties passed to the child. */
  private static final List<String> PASSED_PROPERTY_PREFIXES = Collections.unmodifiableList(Arrays.asList("hoodie.", "spark."));

  private ScenarioJvm() {
  }

  /** The outcome of one child run. */
  public static final class Result {
    public final int exitCode;
    /** One line per finished test or failed container: {@code <STATUS>\t<method> <display name>}, then its stack trace. */
    public final List<String> report;
    public final Path log;

    Result(int exitCode, List<String> report, Path log) {
      this.exitCode = exitCode;
      this.report = report;
      this.log = log;
    }

    /** The report lines of tests with {@code status} (SUCCESSFUL, FAILED, ABORTED, SKIPPED). */
    public List<String> withStatus(String status) {
      return report.stream().filter(line -> line.startsWith(status + "\t")).collect(Collectors.toList());
    }

    /** The report, then the tail of the child's log: what a failure message shows. */
    public String describe() {
      List<String> logLines;
      try {
        logLines = Files.exists(log) ? Files.readAllLines(log, StandardCharsets.UTF_8) : Collections.emptyList();
      } catch (IOException e) {
        logLines = Collections.singletonList("cannot read " + log + ": " + e);
      }
      List<String> tail = logLines.subList(Math.max(0, logLines.size() - LOG_TAIL_LINES), logLines.size());
      return "child exit " + exitCode + "\n--- report\n" + String.join("\n", report) + "\n--- tail of " + log + "\n"
          + String.join("\n", tail);
    }
  }

  /**
   * Runs {@code scenarioClass} in a child JVM without FFmpeg, only the methods named {@code methods} when any are given.
   *
   * @param workDir a directory for the child's argument file, report and log
   */
  public static Result run(Path workDir, String scenarioClass, String... methods) throws IOException, InterruptedException {
    List<String> classPath = NoFfmpegClassPath.testClassPathWithoutFfmpeg();
    if (classPath.stream().noneMatch(entry -> entry.replace('\\', '/').endsWith("target/test-classes"))
        || classPath.stream().noneMatch(entry -> entry.contains("jcodec"))) {
      throw new IllegalStateException("the class path without FFmpeg lost the test classes or JCodec: " + classPath);
    }
    Path argFile = workDir.resolve("child-jvm.args");
    Path reportFile = workDir.resolve("child-report.txt");
    Path log = workDir.resolve("child-output.log");
    // the class path is far longer than one command line argument may be: the JVM reads it from an argument file
    Files.write(argFile, Arrays.asList("-cp", quote(String.join(File.pathSeparator, classPath))), StandardCharsets.UTF_8);

    List<String> command = new ArrayList<>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    command.addAll(jvmOptions());
    command.add("@" + argFile);
    command.add(ScenarioJvm.class.getName());
    command.add(reportFile.toString());
    command.add(scenarioClass);
    command.addAll(Arrays.asList(methods));

    Process child = new ProcessBuilder(command).directory(new File(System.getProperty("user.dir")))
        .redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      if (!child.waitFor(TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
        child.destroyForcibly().waitFor();
        List<String> report = Collections.singletonList("TIMEOUT\tchild JVM still running after " + TIMEOUT_MINUTES + " minutes");
        return new Result(-1, report, log);
      }
    } finally {
      // an interrupted or failing wait never leaves the child running
      if (child.isAlive()) {
        child.destroyForcibly();
      }
    }
    List<String> report = Files.exists(reportFile) ? Files.readAllLines(reportFile, StandardCharsets.UTF_8)
        : Collections.singletonList("NO-REPORT\tthe child wrote no report");
    return new Result(child.exitValue(), report, log);
  }

  /**
   * This JVM's options without a debugger or java agent, then the passed system properties as they are set now (surefire sets
   * its configured ones after start, not on the command line).
   */
  private static List<String> jvmOptions() {
    List<String> options = new ArrayList<>();
    for (String option : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
      if (option.startsWith("-agentlib:") || option.startsWith("-javaagent:")) {
        continue;
      }
      options.add(option);
    }
    for (String name : System.getProperties().stringPropertyNames()) {
      if (PASSED_PROPERTIES.contains(name) || PASSED_PROPERTY_PREFIXES.stream().anyMatch(name::startsWith)) {
        options.add("-D" + name + "=" + System.getProperty(name));
      }
    }
    return options;
  }

  /** An argument-file token: double quoted, backslashes and quotes escaped. */
  private static String quote(String value) {
    return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  /**
   * The child: {@code <report file> <scenario class> [method...]}. Runs the class's tests (only the named methods when any),
   * writes the report and exits 0 when at least one test ran and none failed.
   */
  public static void main(String[] args) throws IOException {
    Path reportFile = Path.of(args[0]);
    String scenarioClass = args[1];
    Set<String> methods = new HashSet<>(Arrays.asList(args).subList(2, args.length));
    PostDiscoveryFilter methodFilter = descriptor -> {
      TestSource source = descriptor.getSource().orElse(null);
      if (methods.isEmpty() || !(source instanceof MethodSource)) {
        return FilterResult.included("no method filter");
      }
      return FilterResult.includedIf(methods.contains(((MethodSource) source).getMethodName()));
    };
    LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request().selectors(selectClass(scenarioClass))
        .filters(methodFilter).build();
    List<String> report = Collections.synchronizedList(new ArrayList<>());
    SummaryGeneratingListener summary = new SummaryGeneratingListener();
    Launcher launcher = LauncherFactory.create();
    int exitCode;
    try {
      launcher.execute(request, summary, new Recorder(report));
      TestExecutionSummary counts = summary.getSummary();
      report.add("SUMMARY\tfound " + counts.getTestsFoundCount() + " succeeded " + counts.getTestsSucceededCount() + " failed "
          + counts.getTotalFailureCount() + " skipped " + counts.getTestsSkippedCount());
      exitCode = counts.getTotalFailureCount() > 0 ? EXIT_FAILED : counts.getTestsSucceededCount() == 0 ? EXIT_NO_TESTS : 0;
    } catch (RuntimeException | Error e) {
      report.add("LAUNCHER\t" + stackTrace(e));
      exitCode = EXIT_FAILED;
    }
    Files.write(reportFile, report, StandardCharsets.UTF_8);
    // Spark leaves non-daemon threads behind: the child ends here
    System.exit(exitCode);
  }

  /** Records the outcome of every test, and of every container that did not succeed (a failed {@code @BeforeAll}). */
  private static final class Recorder implements TestExecutionListener {
    private final List<String> report;

    Recorder(List<String> report) {
      this.report = report;
    }

    @Override
    public void executionSkipped(TestIdentifier test, String reason) {
      report.add("SKIPPED\t" + name(test) + ": " + reason);
    }

    @Override
    public void executionFinished(TestIdentifier test, TestExecutionResult result) {
      if (!test.isTest() && result.getStatus() == TestExecutionResult.Status.SUCCESSFUL) {
        return;
      }
      StringBuilder line = new StringBuilder(result.getStatus() + "\t" + name(test));
      result.getThrowable().ifPresent(failure -> line.append('\n').append(stackTrace(failure)));
      report.add(line.toString());
    }

    private static String name(TestIdentifier test) {
      return test.getSource().filter(MethodSource.class::isInstance).map(source -> ((MethodSource) source).getMethodName() + " ")
          .orElse("") + test.getDisplayName();
    }
  }

  private static String stackTrace(Throwable failure) {
    StringWriter out = new StringWriter();
    failure.printStackTrace(new PrintWriter(out));
    return out.toString().trim();
  }
}
