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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

  private static final Pattern WILDCARD_PARAM_NAME = Pattern.compile("^\\s*- name:\\s*(\\S+)\\s*$");
  private static final Pattern WILDCARD_LIST_ITEM = Pattern.compile("^\\s*- '([^']+)'\\s*$");
  private static final Pattern WILDCARD_CLI_ARG = Pattern.compile("-DwildcardSuites=\"?([^\" ]+)");

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
   * Every Scala test class under {@link #BASE_PACKAGE} must be named by at least one Azure
   * wildcardSuites entry, otherwise it silently never runs on Azure.
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
  public void testScalaTestPackagesAreCoveredByAzureWildcardSuites() {
    Set<String> azurePrefixes = readAzureWildcardSuitePrefixes();
    assertFalse(azurePrefixes.isEmpty(),
        "Expected to parse at least one wildcardSuites entry from " + AZURE_PIPELINE_FILE);

    List<String> uncovered = findScalaTestClasses().stream()
        .filter(className -> azurePrefixes.stream()
            .noneMatch(prefix -> className.equals(prefix) || className.startsWith(prefix + ".")))
        .collect(Collectors.toList());

    if (!uncovered.isEmpty()) {
      StringBuilder message = new StringBuilder();
      message.append("Found Scala test classes that no Azure wildcardSuites entry covers, ")
          .append("so they never run on Azure CI.\n\nUncovered classes:\n");
      uncovered.forEach(cls -> message.append("  - ").append(cls).append("\n"));
      message.append("\nAdd their package to one of the wildcardSuites sets in ")
          .append(AZURE_PIPELINE_FILE)
          .append(" (e.g. 'job6HudiSparkDdlOthersWildcardSuites'), balancing against job runtime.");
      fail(message.toString());
    }
  }

  /**
   * Collects every package prefix any Azure job passes to {@code -DwildcardSuites}, both the
   * inline arguments and the entries of the {@code *WildcardSuites} parameter lists they expand.
   */
  private Set<String> readAzureWildcardSuitePrefixes() {
    File projectRoot = findProjectRoot();
    if (projectRoot == null) {
      fail("Could not locate project root directory");
      return new HashSet<>();
    }
    File pipeline = new File(projectRoot, AZURE_PIPELINE_FILE);
    if (!pipeline.exists()) {
      fail("Could not locate " + AZURE_PIPELINE_FILE + " at " + pipeline.getAbsolutePath());
      return new HashSet<>();
    }

    List<String> lines;
    try {
      lines = Files.readAllLines(pipeline.toPath(), StandardCharsets.UTF_8);
    } catch (Exception e) {
      fail("Could not read " + AZURE_PIPELINE_FILE + ": " + e.getMessage());
      return new HashSet<>();
    }

    Set<String> prefixes = new HashSet<>();
    boolean inWildcardList = false;
    for (String line : lines) {
      Matcher paramName = WILDCARD_PARAM_NAME.matcher(line);
      if (paramName.matches()) {
        inWildcardList = paramName.group(1).endsWith("WildcardSuites");
        continue;
      }
      Matcher listItem = WILDCARD_LIST_ITEM.matcher(line);
      if (inWildcardList && listItem.matches()) {
        prefixes.add(listItem.group(1).trim());
      }
      Matcher cliArg = WILDCARD_CLI_ARG.matcher(line);
      while (cliArg.find()) {
        for (String suite : cliArg.group(1).split(",")) {
          // skip the $(VAR) expansions, whose entries are picked up from the parameter lists
          if (suite.startsWith("org.")) {
            prefixes.add(suite.trim());
          }
        }
      }
    }
    return prefixes;
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
    List<String> classes = new ArrayList<>();

    // Find project root by traversing up from current class location
    File projectRoot = findProjectRoot();
    if (projectRoot == null) {
      fail("Could not locate project root directory");
      return classes;
    }

    // Scan all hudi-spark-datasource modules for Scala test sources
    File sparkDatasourceDir = new File(projectRoot, "hudi-spark-datasource");
    if (!sparkDatasourceDir.exists()) {
      fail("Could not locate hudi-spark-datasource directory");
      return classes;
    }

    // Look for Scala test directories in all submodules
    File[] submodules = sparkDatasourceDir.listFiles(File::isDirectory);
    if (submodules != null) {
      for (File submodule : submodules) {
        File scalaTestDir = new File(submodule, "src/test/scala/" + PACKAGE_PATH);
        if (scalaTestDir.exists() && scalaTestDir.isDirectory()) {
          classes.addAll(findScalaFilesRecursively(scalaTestDir, BASE_PACKAGE));
        }
      }
    }

    return classes;
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
