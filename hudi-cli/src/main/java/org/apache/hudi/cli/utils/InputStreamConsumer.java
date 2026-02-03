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

package org.apache.hudi.cli.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * This class is responsible to read a Process output.
 */
@Slf4j
public class InputStreamConsumer extends Thread {
  private final InputStream is;

  public InputStreamConsumer(InputStream is) {
    this.is = is;
  }

  @Override
  public void run() {
    try {
      InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8);
      BufferedReader br = new BufferedReader(isr);
      br.lines().forEach(log::info);
    } catch (Exception e) {
      log.warn("Error consuming input stream", e);
    }
  }

  public static void captureOutput(Process p) {
    InputStreamConsumer stdout;
    InputStreamConsumer errout;
    errout = new InputStreamConsumer(p.getErrorStream());
    stdout = new InputStreamConsumer(p.getInputStream());
    errout.start();
    stdout.start();
  }

}
