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
  private static final String AZURE_PIPELINE_FILE = "azure-pipelines-20230430.yml";
  private static final String SPARK_DATASOURCE_DIR = "hudi-spark-datasource";

  private static final Pattern PARAM_NAME = Pattern.compile("^\\s*- name:\\s*(\\S+)\\s*$");
  private static final Pattern LIST_ITEM = Pattern.compile("^\\s*- '([^']*)'\\s*$");
  /** A {@code variables:} entry that is just a join over a parameter list, e.g. {@code
   * JOB3456_MODULES: ${{ join(',',parameters.job3456UTModules) }}}. */
  private static final Pattern VARIABLE_JOIN =
      Pattern.compile("^\\s*([A-Za-z0-9_]+):\\s*\\$\\{\\{\\s*join\\('[^']*',\\s*parameters\\.([A-Za-z0-9_]+)\\)\\s*\\}\\}\\s*$");
  private static final Pattern WILDCARD_CLI_ARG = Pattern.compile("-DwildcardSuites=\"?([^\" ]+)");
  private static final Pattern PL_CLI_ARG = Pattern.compile("-pl\\s+\"?([^\" ]+)");
  private static final Pattern VAR_REF = Pattern.compile("\\$\\(([A-Za-z0-9_]+)\\)");

  /**
   * Allowed sub-packages under org.apache.spark.sql.hudi for Scala test classes.
   * This list **MUST** be kept in sync with:
   * (1) The 'job6HudiSparkDdlOthersWildcardSuites' list in azure-pipelines-20230430.yml for Azure
   * CI (excluding org.apache.spark.sql.hudi.dml and org.apache.spark.sql.hudi.feature)
   * (2) the Scala other test filter (SCALA_TEST_OTHERS_FILTER) in .github/workflows/bot.yml
   * for GitHub actions (excluding org.apache.spark.sql.hudi.dml)
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
          .append("or add the new package to the allowed list in both this test ")
          .append("and azure-pipelines-20230430.yml (job6HudiSparkDdlOthersWildcardSuites).");
      fail(message.toString());
    }

    assertFalse(scalaTestClasses.isEmpty(),
        "Expected to find at least one Scala test class in " + BASE_PACKAGE);
  }

  /**
   * Every Scala test class under {@link #BASE_PACKAGE} must be run by at least one Azure step:
   * some step whose {@code -pl} builds the module holding the class must also pass a
   * {@code -DwildcardSuites} prefix matching it. Otherwise the class silently never runs on Azure.
   *
   * <p>The Azure jobs deliberately name leaf packages ({@code dml.others}, {@code dml.insert},
   * {@code dml.schema}) rather than the recursive {@code dml} parent, because ScalaTest's
   * {@code -w} is a plain prefix match with no exclusion primitive: pointing one job at
   * {@code ...hudi.dml} would re-run the whole {@code dml.insert} set that already has its own
   * job. That split is what makes a newly added {@code dml.*} package start out dark, so this
   * test is the guard for it - the other, non-recursive {@code testSparkSqlHudi...} check above
   * lets {@code dml.*} through because it treats {@code dml} as one allowed package.
   */
  @Test
  public void testScalaTestPackagesAreRunByAnAzureStep() {
    List<AzureScalaTestStep> steps = readAzureScalaTestSteps();
    assertFalse(steps.isEmpty(),
        "Expected to parse at least one -DwildcardSuites step from " + AZURE_PIPELINE_FILE
            + "; the parsing below has probably drifted from the pipeline's shape");

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
      message.append("Found Scala test classes that no Azure step runs - no step both builds ")
          .append("their module (-pl) and names their package (-DwildcardSuites), so they never ")
          .append("run on Azure CI.\n\nUncovered classes:\n");
      uncovered.forEach(cls -> message.append("  - ").append(cls).append("\n"));
      message.append("\nAdd their package to one of the wildcardSuites sets in ")
          .append(AZURE_PIPELINE_FILE)
          .append(" (e.g. 'job6HudiSparkDdlOthersWildcardSuites'), balancing against job runtime.");
      fail(message.toString());
    }
  }

  /**
   * One Azure step that runs ScalaTest: the suite prefixes it passes to {@code -DwildcardSuites}
   * and the module list it passes to {@code -pl}, both with {@code $(VAR)} references resolved
   * back to the parameter list the {@code variables:} block joins them from.
   */
  private static final class AzureScalaTestStep {
    private final List<String> suitePrefixes;
    private final List<String> modules;

    private AzureScalaTestStep(List<String> suitePrefixes, List<String> modules) {
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
   * Parses the pipeline into the ScalaTest steps it defines. Resolves {@code $(VAR)} tokens
   * through the {@code variables:} block back to the {@code parameters:} list they join, so a
   * parameter list that no step actually expands contributes nothing.
   */
  private List<AzureScalaTestStep> readAzureScalaTestSteps() {
    List<String> lines = readAzurePipelineLines();

    // parameters: <name> -> its list of values
    Map<String, List<String>> parameterLists = new HashMap<>();
    String currentParam = null;
    for (String line : lines) {
      Matcher paramName = PARAM_NAME.matcher(line);
      if (paramName.matches()) {
        currentParam = paramName.group(1);
        parameterLists.put(currentParam, new ArrayList<>());
        continue;
      }
      Matcher listItem = LIST_ITEM.matcher(line);
      if (currentParam != null && listItem.matches()) {
        parameterLists.get(currentParam).add(listItem.group(1).trim());
      }
    }

    // variables: <VAR> -> the parameter list it joins
    Map<String, List<String>> variableValues = new HashMap<>();
    for (String line : lines) {
      Matcher variable = VARIABLE_JOIN.matcher(line);
      if (variable.matches()) {
        List<String> values = parameterLists.get(variable.group(2));
        if (values != null) {
          variableValues.put(variable.group(1), values);
        }
      }
    }

    // steps: every line carrying both a wildcardSuites filter and a -pl module list
    List<AzureScalaTestStep> steps = new ArrayList<>();
    for (String line : lines) {
      Matcher suites = WILDCARD_CLI_ARG.matcher(line);
      Matcher modules = PL_CLI_ARG.matcher(line);
      if (!suites.find() || !modules.find()) {
        continue;
      }
      List<String> suitePrefixes = expand(suites.group(1), variableValues).stream()
          .filter(suite -> suite.startsWith("org."))
          .collect(Collectors.toList());
      if (suitePrefixes.isEmpty()) {
        // e.g. the java-only steps, which pass -DwildcardSuites=skipScalaTests
        continue;
      }
      steps.add(new AzureScalaTestStep(suitePrefixes, expand(modules.group(1), variableValues)));
    }
    return steps;
  }

  /**
   * Splits a comma-separated Azure argument into its entries, replacing any {@code $(VAR)} token
   * with the entries of the parameter list that variable joins.
   */
  private List<String> expand(String argument, Map<String, List<String>> variableValues) {
    List<String> expanded = new ArrayList<>();
    for (String token : argument.split(",")) {
      String trimmed = token.trim();
      Matcher varRef = VAR_REF.matcher(trimmed);
      if (varRef.matches()) {
        expanded.addAll(variableValues.getOrDefault(varRef.group(1), new ArrayList<>()));
      } else if (!trimmed.isEmpty()) {
        expanded.add(trimmed);
      }
    }
    return expanded;
  }

  private List<String> readAzurePipelineLines() {
    File projectRoot = findProjectRoot();
    if (projectRoot == null) {
      fail("Could not locate project root directory");
      return new ArrayList<>();
    }
    File pipeline = new File(projectRoot, AZURE_PIPELINE_FILE);
    if (!pipeline.exists()) {
      fail("Could not locate " + AZURE_PIPELINE_FILE + " at " + pipeline.getAbsolutePath());
      return new ArrayList<>();
    }
    try {
      return Files.readAllLines(pipeline.toPath(), StandardCharsets.UTF_8);
    } catch (Exception e) {
      fail("Could not read " + AZURE_PIPELINE_FILE + ": " + e.getMessage());
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
   * that holds them (as Azure would name it in {@code -pl}), e.g.
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
