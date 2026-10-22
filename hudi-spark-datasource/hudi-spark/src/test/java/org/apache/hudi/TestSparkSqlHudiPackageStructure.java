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

package org.apache.hudi;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Validates that Scala test classes under org.apache.spark.sql.hudi are only in allowed packages.
 * This ensures proper package structure for CI wildcard suite configurations.
 * Only Scala test classes are checked; Java test classes are excluded.
 */
public class TestSparkSqlHudiPackageStructure {

  private static final String BASE_PACKAGE = "org.apache.spark.sql.hudi";
  private static final String PACKAGE_PATH = "org/apache/spark/sql/hudi";
  private static final String SPARK35_WORKFLOW_FILE = ".github/workflows/spark35_ci.yml";
  private static final String SPARK_DATASOURCE_DIR = "hudi-spark-datasource";

  /** An {@code env:} entry, e.g. {@code SCALA_TEST_DML_FILTER: -DwildcardSuites=...}. */
  private static final Pattern ENV_ENTRY = Pattern.compile("^\\s*([A-Z][A-Z0-9_]*):\\s*(\\S.*?)\\s*$");
  /** A shell variable reference, {@code $NAME} or {@code ${NAME}}. */
  private static final Pattern SHELL_VAR_REF =
      Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)\\}|\\$([A-Za-z_][A-Za-z0-9_]*)");
  private static final Pattern WILDCARD_CLI_ARG = Pattern.compile("-DwildcardSuites=\"?([^\" ]+)");
  private static final Pattern PL_CLI_ARG = Pattern.compile("-pl\\s+\"?([^\" ]+)");

  /**
   * Allowed sub-packages under org.apache.spark.sql.hudi for Scala test classes.
   * This list **MUST** be kept in sync with the Scala test filters (SCALA_TEST_OTHERS_FILTER,
   * plus SCALA_TEST_DML_FILTER for org.apache.spark.sql.hudi.dml) in both
   * .github/workflows/bot.yml and .github/workflows/spark35_ci.yml.
   */
  private static final Set<String> ALLOWED_PACKAGES = new HashSet<>(Arrays.asList(
      "org.apache.spark.sql.hudi.analysis",
      "org.apache.spark.sql.hudi.blob",
      "org.apache.spark.sql.hudi.catalog",
      "org.apache.spark.sql.hudi.command",
      "org.apache.spark.sql.hudi.common",
      "org.apache.spark.sql.hudi.ddl",
      "org.apache.spark.sql.hudi.dml",
      "org.apache.spark.sql.hudi.feature",
      "org.apache.spark.sql.hudi.procedure"
  ));

  @Test
  public void testSparkSqlHudiScalaTestClassesInAllowedPackagesOnly() {
    List<String> scalaTestClasses = findScalaTestClasses();

    List<String> violatingClasses = scalaTestClasses.stream()
        .filter(className -> !isInAllowedPackage(className))
        .collect(Collectors.toList());

    if (!violatingClasses.isEmpty()) {
      StringBuilder message = new StringBuilder();
      message.append("Found Scala test classes under '").append(BASE_PACKAGE)
          .append("' that are not in any of the allowed packages.\n\n");
      message.append("Allowed packages:\n");
      ALLOWED_PACKAGES.forEach(pkg -> message.append("  - ").append(pkg).append("\n"));
      message.append("\nViolating classes:\n");
      violatingClasses.forEach(cls -> message.append("  - ").append(cls).append("\n"));
      message.append("\nPlease move these test classes to one of the allowed packages, ")
          .append("or add the new package to the allowed list in this test and to ")
          .append("SCALA_TEST_OTHERS_FILTER in both .github/workflows/bot.yml and ")
          .append(SPARK35_WORKFLOW_FILE).append(".");
      fail(message.toString());
    }

    assertFalse(scalaTestClasses.isEmpty(),
        "Expected to find at least one Scala test class in " + BASE_PACKAGE);
  }

  /**
   * Guarantees that no Scala test class under {@link #BASE_PACKAGE} is left out of the Spark 3.5
   * run in {@link #SPARK35_WORKFLOW_FILE}: some {@code mvn test} step whose {@code -pl} builds the
   * module holding the class must also pass a {@code -DwildcardSuites} prefix matching it. That
   * workflow is read because its filters are spelled out in {@code env:}; a class matched by no
   * filter would never run on Spark 3.5.
   */
  @Test
  public void testScalaTestPackagesAreRunBySpark35Step() {
    List<ScalaTestStep> steps = readScalaTestSteps();
    assertFalse(steps.isEmpty(),
        "Expected to parse at least one -DwildcardSuites step from " + SPARK35_WORKFLOW_FILE
            + "; the parsing below has probably drifted from the workflow's shape");

    Map<String, List<String>> classesByModule = findScalaTestClassesByModule();
    List<String> uncovered = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry : classesByModule.entrySet()) {
      String module = entry.getKey();
      for (String className : entry.getValue()) {
        boolean run = steps.stream().anyMatch(step -> step.builds(module) && step.runs(className));
        if (!run) {
          uncovered.add(className + "  (module " + module + ")");
        }
      }
    }

    if (!uncovered.isEmpty()) {
      StringBuilder message = new StringBuilder();
      message.append("Found Scala test classes that no step of ").append(SPARK35_WORKFLOW_FILE)
          .append(" runs - no mvn test step both builds their module (-pl) and names their ")
          .append("package (-DwildcardSuites), so they never run on Spark 3.5.\n\nUncovered classes:\n");
      uncovered.forEach(cls -> message.append("  - ").append(cls).append("\n"));
      message.append("\nAdd their package to one of the -DwildcardSuites filters in the env: block of ")
          .append(SPARK35_WORKFLOW_FILE)
          .append(" (e.g. SCALA_TEST_OTHERS_FILTER); for a new package, also add it to ")
          .append("SCALA_TEST_OTHERS_FILTER in .github/workflows/bot.yml.");
      fail(message.toString());
    }
  }

  /**
   * One workflow step that runs ScalaTest: the suite prefixes it passes to {@code -DwildcardSuites}
   * and the module list it passes to {@code -pl}, after expanding {@code env:} variables.
   */
  private static final class ScalaTestStep {
    private final List<String> suitePrefixes;
    private final List<String> modules;

    private ScalaTestStep(List<String> suitePrefixes, List<String> modules) {
      this.suitePrefixes = suitePrefixes;
      this.modules = modules;
    }

    /** Whether this step's {@code -pl} selects the given module. */
    private boolean builds(String module) {
      if (modules.isEmpty()) {
        // no -pl at all means the whole reactor
        return true;
      }
      if (modules.contains("!" + module)) {
        return false;
      }
      // an all-exclusion list selects everything it does not name; otherwise it is an include list
      boolean allExclusions = modules.stream().allMatch(m -> m.startsWith("!"));
      return allExclusions || modules.contains(module);
    }

    /** Whether this step's {@code -DwildcardSuites} names the given class. ScalaTest's -w is a
     * prefix match; the dot boundary keeps this check strictly narrower than what -w accepts. */
    private boolean runs(String className) {
      return suitePrefixes.stream()
          .anyMatch(prefix -> className.equals(prefix) || className.startsWith(prefix + "."));
    }
  }

  /**
   * Parses the workflow into the ScalaTest steps it defines. {@code $NAME} / {@code ${NAME}}
   * tokens in {@code mvn test} lines are expanded from the workflow's {@code env:} entries;
   * entries whose value is a {@code ${{ ... }}} expression (e.g. matrix values) stay unresolved.
   */
  private List<ScalaTestStep> readScalaTestSteps() {
    List<String> lines = readWorkflowLines();

    Map<String, String> envValues = new HashMap<>();
    for (String line : lines) {
      Matcher entry = ENV_ENTRY.matcher(line);
      if (entry.matches() && !entry.group(2).startsWith("${{")) {
        envValues.put(entry.group(1), entry.group(2));
      }
    }

    List<ScalaTestStep> steps = new ArrayList<>();
    for (String line : lines) {
      if (!line.contains("mvn test")) {
        continue;
      }
      String expanded = expandEnvVars(line, envValues);
      Matcher suites = WILDCARD_CLI_ARG.matcher(expanded);
      Matcher modules = PL_CLI_ARG.matcher(expanded);
      if (!suites.find() || !modules.find()) {
        continue;
      }
      List<String> suitePrefixes = splitList(suites.group(1)).stream()
          .filter(suite -> suite.startsWith("org."))
          .collect(Collectors.toList());
      if (suitePrefixes.isEmpty()) {
        // e.g. the java-only steps, which pass -DwildcardSuites=skipScalaTests
        continue;
      }
      steps.add(new ScalaTestStep(suitePrefixes, splitList(modules.group(1))));
    }
    return steps;
  }

  /** Replaces each shell variable reference whose name is in {@code envValues} with its value. */
  private String expandEnvVars(String line, Map<String, String> envValues) {
    Matcher varRef = SHELL_VAR_REF.matcher(line);
    StringBuffer expanded = new StringBuffer();
    while (varRef.find()) {
      String name = varRef.group(1) != null ? varRef.group(1) : varRef.group(2);
      String value = envValues.getOrDefault(name, varRef.group());
      varRef.appendReplacement(expanded, Matcher.quoteReplacement(value));
    }
    varRef.appendTail(expanded);
    return expanded.toString();
  }

  /** Splits a comma-separated argument into its non-empty entries. */
  private List<String> splitList(String argument) {
    List<String> entries = new ArrayList<>();
    for (String token : argument.split(",")) {
      String trimmed = token.trim();
      if (!trimmed.isEmpty()) {
        entries.add(trimmed);
      }
    }
    return entries;
  }

  private List<String> readWorkflowLines() {
    File projectRoot = findProjectRoot();
    if (projectRoot == null) {
      fail("Could not locate project root directory");
      return new ArrayList<>();
    }
    File workflow = new File(projectRoot, SPARK35_WORKFLOW_FILE);
    if (!workflow.exists()) {
      fail("Could not locate " + SPARK35_WORKFLOW_FILE + " at " + workflow.getAbsolutePath());
      return new ArrayList<>();
    }
    try {
      return Files.readAllLines(workflow.toPath(), StandardCharsets.UTF_8);
    } catch (Exception e) {
      fail("Could not read " + SPARK35_WORKFLOW_FILE + ": " + e.getMessage());
      return new ArrayList<>();
    }
  }

  /**
   * Checks if a class is in one of the allowed packages (including sub-packages).
   */
  private boolean isInAllowedPackage(String className) {
    for (String allowedPackage : ALLOWED_PACKAGES) {
      if (className.startsWith(allowedPackage + ".")) {
        return true;
      }
    }
    return false;
  }

  /**
   * Finds all Scala test classes under org.apache.spark.sql.hudi by scanning source directories.
   */
  private List<String> findScalaTestClasses() {
    return findScalaTestClassesByModule().values().stream()
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  /**
   * Finds all Scala test classes under org.apache.spark.sql.hudi, keyed by the Maven module path
   * that holds them (as a workflow would name it in {@code -pl}), e.g.
   * {@code hudi-spark-datasource/hudi-spark}.
   */
  private Map<String, List<String>> findScalaTestClassesByModule() {
    Map<String, List<String>> classesByModule = new TreeMap<>();

    // Find project root by traversing up from current class location
    File projectRoot = findProjectRoot();
    if (projectRoot == null) {
      fail("Could not locate project root directory");
      return classesByModule;
    }

    // Scan all hudi-spark-datasource modules for Scala test sources
    File sparkDatasourceDir = new File(projectRoot, SPARK_DATASOURCE_DIR);
    if (!sparkDatasourceDir.exists()) {
      fail("Could not locate " + SPARK_DATASOURCE_DIR + " directory");
      return classesByModule;
    }

    // Look for Scala test directories in all submodules
    File[] submodules = sparkDatasourceDir.listFiles(File::isDirectory);
    if (submodules != null) {
      for (File submodule : submodules) {
        File scalaTestDir = new File(submodule, "src/test/scala/" + PACKAGE_PATH);
        if (scalaTestDir.exists() && scalaTestDir.isDirectory()) {
          List<String> classes = findScalaFilesRecursively(scalaTestDir, BASE_PACKAGE);
          if (!classes.isEmpty()) {
            classesByModule.put(SPARK_DATASOURCE_DIR + "/" + submodule.getName(), classes);
          }
        }
      }
    }

    return classesByModule;
  }

  /**
   * Finds the project root directory by traversing up from the compiled class location.
   */
  private File findProjectRoot() {
    try {
      // Get the location of this compiled test class
      Path classPath = Paths.get(getClass().getProtectionDomain().getCodeSource().getLocation().toURI());

      // Traverse up to find the project root (look for hudi-spark-datasource directory)
      File current = classPath.toFile();
      while (current != null) {
        File sparkDatasource = new File(current, "hudi-spark-datasource");
        if (sparkDatasource.exists() && sparkDatasource.isDirectory()) {
          return current;
        }
        current = current.getParentFile();
      }
    } catch (Exception e) {
      // Fall back to working directory
      File current = new File(System.getProperty("user.dir"));
      while (current != null) {
        File sparkDatasource = new File(current, "hudi-spark-datasource");
        if (sparkDatasource.exists() && sparkDatasource.isDirectory()) {
          return current;
        }
        current = current.getParentFile();
      }
    }
    return null;
  }

  /**
   * Recursively finds all Scala files in a directory.
   */
  private List<String> findScalaFilesRecursively(File directory, String packageName) {
    List<String> classes = new ArrayList<>();
    if (!directory.exists()) {
      return classes;
    }

    File[] files = directory.listFiles();
    if (files == null) {
      return classes;
    }

    for (File file : files) {
      if (file.isDirectory()) {
        classes.addAll(findScalaFilesRecursively(file, packageName + "." + file.getName()));
      } else if (file.getName().endsWith(".scala")) {
        String className = packageName + '.' + file.getName().substring(0, file.getName().length() - 6);
        // Filter for test classes only
        if (isTestClass(className)) {
          classes.add(className);
        }
      }
    }
    return classes;
  }

  /**
   * Determines if a class is a test class based on naming conventions.
   */
  private boolean isTestClass(String className) {
    String simpleName = className.substring(className.lastIndexOf('.') + 1);
    // Include classes that start with "Test", end with "Test", or are test base classes
    return simpleName.startsWith("Test")
        || simpleName.endsWith("Test")
        || simpleName.endsWith("TestBase");
  }
}
