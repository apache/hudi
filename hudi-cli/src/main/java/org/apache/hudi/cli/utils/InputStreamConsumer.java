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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * This class is responsible to read a Process output.
 */
public class InputStreamConsumer extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(InputStreamConsumer.class);
  private final InputStream is;

  public InputStreamConsumer(InputStream is) {
    this.is = is;
  }

  @Override
  public void run() {
    try {
      InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8);
      BufferedReader br = new BufferedReader(isr);
      br.lines().forEach(LOG::info);
    } catch (Exception e) {
      LOG.warn(e.toString());
      e.printStackTrace();
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
