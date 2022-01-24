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

package org.apache.hudi.common.util;

import org.apache.hudi.exception.HoodieException;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;

/**
 * A utility class for network.
 */
public class NetworkUtils {

  public static synchronized String getHostname() {
    try (DatagramSocket s = new DatagramSocket()) {
      // see https://stackoverflow.com/questions/9481865/getting-the-ip-address-of-the-current-machine-using-java
      // for details.
      s.connect(InetAddress.getByName("8.8.8.8"), 10002);
      return s.getLocalAddress().getHostAddress();
    } catch (IOException e) {
      throw new HoodieException("Unable to find server port", e);
    }
  }
}
