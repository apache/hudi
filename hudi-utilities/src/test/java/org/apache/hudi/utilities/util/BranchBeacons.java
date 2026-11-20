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

package org.apache.hudi.utilities.util;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Reads which adjudication rows a piece of code took, off the {@link BranchBeacon} log.
 *
 * <p>One home for the capture, shared by the unit suites here and — through
 * {@code ValidatorV2FunctionalTestBase.ObservedRun}, which captures the same logger as part of a whole
 * run's log — by the functional ones. Two copies of this would be worth less than one: the failure
 * mode of a broken collector is that every {@code took…} assertion silently passes, which is
 * indistinguishable from the coverage those assertions exist to establish.
 */
final class BranchBeacons {

  private BranchBeacons() {
  }

  /**
   * The branches {@code body} took.
   *
   * <p>The beacon's level is forced to DEBUG for the duration rather than inherited from the surefire
   * log4j config: that config is shared with every other module, and a future tightening of it would
   * make every assertion built on this method vacuous without failing anything.
   */
  static Set<Branch> takenBy(Runnable body) {
    Logger logger = (Logger) LogManager.getLogger(BranchBeacon.class);
    Collector collector = new Collector();
    Level before = logger.getLevel();
    logger.addAppender(collector);
    logger.setLevel(Level.DEBUG);
    try {
      body.run();
    } finally {
      logger.setLevel(before);
      logger.removeAppender(collector);
    }
    return parse(collector.messages);
  }

  /** Asserts {@code body} took {@code expected}, naming what it took instead when it did not. */
  static void assertTakes(Branch expected, Runnable body) {
    Set<Branch> taken = takenBy(body);
    assertTrue(taken.contains(expected),
        "expected the branch " + expected + " to be taken; the code took " + taken);
  }

  private static Set<Branch> parse(List<String> messages) {
    Set<Branch> branches = EnumSet.noneOf(Branch.class);
    for (String message : messages) {
      int at = message.indexOf(BranchBeacon.PREFIX);
      if (at < 0) {
        continue;
      }
      String id = message.substring(at + BranchBeacon.PREFIX.length()).trim();
      try {
        branches.add(Branch.valueOf(id));
      } catch (IllegalArgumentException e) {
        fail("the log carries a branch beacon the Branch enum does not name: '" + id + "'. Either a "
            + "constant was removed without removing its BranchBeacon.hit call, or the beacon line "
            + "format changed and every branch assertion is now vacuous");
      }
    }
    return branches;
  }

  private static final class Collector extends AbstractAppender {

    private final List<String> messages = new ArrayList<>();

    private Collector() {
      super("branchBeaconCollector", null, null, true, null);
      start();
    }

    @Override
    public synchronized void append(LogEvent event) {
      messages.add(event.getMessage().getFormattedMessage());
    }
  }
}
