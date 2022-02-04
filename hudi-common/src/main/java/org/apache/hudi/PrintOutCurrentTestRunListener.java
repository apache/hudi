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

package org.apache.hudi;

import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;

/**
 * @author Paul Verest
 */
public class PrintOutCurrentTestRunListener extends RunListener {
  @Override
  public void testRunStarted(Description description) throws Exception {
    // TODO all methods return null
    System.out.println("testRunStarted " + description.getClassName() + " " + description.getDisplayName() + " "
        + description.toString());
  }

  public void testStarted(Description description) throws Exception {
    System.out.println("testStarted "
        + description.toString());
  }

  public void testFinished(Description description) throws Exception {
    System.out.println("testFinished "
        + description.toString());
  }

  public void testRunFinished(Result result) throws Exception {
    System.out.println("testRunFinished " + result.toString()
        + " time:" + result.getRunTime()
        + " R" + result.getRunCount()
        + " F" + result.getFailureCount()
        + " I" + result.getIgnoreCount()
    );
  }
}