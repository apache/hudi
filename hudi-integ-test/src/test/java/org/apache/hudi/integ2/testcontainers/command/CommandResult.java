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

package org.apache.hudi.integ2.testcontainers.command;

import lombok.AllArgsConstructor;
import org.testcontainers.containers.Container;

import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A dedicated class to hold the result of a command execution and provide
 * fluent assertion methods for cleaner tests.
 */
@AllArgsConstructor
public class CommandResult {

  // Spark 4.0 (Scala 2.13) spark-shell runs under a dumb terminal when stdin
  // is piped from a file, which flushes the next `scala> ` prompt onto the
  // same line as preceding async println output (e.g. `scala> MARKER`).
  // Stripping one-or-more leading `scala>\s+` prefixes normalizes those lines
  // back to the bare sentinel while leaving Spark 3.5 output (sentinel already
  // on its own line) unchanged.
  private static final Pattern REPL_PROMPT_PREFIX = Pattern.compile("^(scala>\\s+)+");

  private final String stdout;
  private final String stderr;
  private final int exitCode;

  public CommandResult(Container.ExecResult execResult) {
    this.stdout = execResult.getStdout();
    this.stderr = execResult.getStderr();
    this.exitCode = execResult.getExitCode();
  }

  /**
   * Asserts that the command's exit code is 0 (success).
   *
   * @return The same {@link CommandResult} instance for chaining assertions.
   * @throws AssertionError if the exit code is not 0.
   */
  public CommandResult expectToSucceed() {
    assertEquals(0, exitCode,
        String.format("Command failed with exit code %d. Stderr: %s", exitCode, stderr));
    return this;
  }

  /**
   * Asserts that the standard output contains a specific substring exactly once.
   *
   * @param expectedSubstring The substring to search for.
   * @return The same {@link CommandResult} instance for chaining assertions.
   */
  public CommandResult assertStdOutContains(String expectedSubstring) {
    return assertStdOutContains(expectedSubstring, 1);
  }

  /**
   * Asserts that the standard output contains a specific substring an exact number of times.
   *
   * @param expectedSubstring The substring to search for.
   * @param times The exact number of times the substring is expected to appear.
   * @return The same {@link CommandResult} instance for chaining assertions.
   */
  public CommandResult assertStdOutContains(String expectedSubstring, int times) {
    // Normalize whitespace for more robust matching
    String stdOutSingleSpaced = stdout.replaceAll("[\\s]+", " ").trim();
    String expectedOutput = expectedSubstring.replaceAll("[\\s]+", " ").trim();

    int lastIndex = 0;
    int count = 0;
    while (lastIndex != -1) {
      lastIndex = stdOutSingleSpaced.indexOf(expectedOutput, lastIndex);
      if (lastIndex != -1) {
        count++;
        lastIndex += expectedOutput.length();
      }
    }
    assertEquals(times, count,
        String.format("Expected to find substring '%s' %d times, but found %d. Full stdout: %s",
            expectedOutput, times, count, stdout));
    return this;
  }

  /**
   * Asserts that the standard output contains a line exactly equal to the expected value
   * (after trimming and after stripping any leading {@code scala> } REPL prompt prefix),
   * appearing exactly once. Use this for REPL sentinel markers emitted via
   * {@code println(...)}. Handles both Scala 2.12's spark-shell echoing the input line
   * back (Spark 3.5) and Scala 2.13's dumb-terminal mode prefixing sentinel output with
   * {@code scala> } on the same line (Spark 4.0).
   *
   * @param expectedLine The exact line content to match (after trim and prompt strip).
   * @return The same {@link CommandResult} instance for chaining assertions.
   */
  public CommandResult assertStdOutContainsLine(String expectedLine) {
    return assertStdOutContainsLine(expectedLine, 1);
  }

  /**
   * Asserts that the standard output contains a line exactly equal to the expected value
   * (after trimming and after stripping any leading {@code scala> } REPL prompt prefix),
   * appearing an exact number of times.
   *
   * @param expectedLine The exact line content to match (after trim and prompt strip).
   * @param times The exact number of matching lines expected.
   * @return The same {@link CommandResult} instance for chaining assertions.
   */
  public CommandResult assertStdOutContainsLine(String expectedLine, int times) {
    String expected = expectedLine.trim();
    long count = stdout.lines()
        .map(line -> REPL_PROMPT_PREFIX.matcher(line).replaceFirst("").trim())
        .filter(expected::equals)
        .count();
    assertEquals(times, count,
        String.format("Expected line '%s' %d times, but found %d. Full stdout: %s",
            expected, times, count, stdout));
    return this;
  }
}
