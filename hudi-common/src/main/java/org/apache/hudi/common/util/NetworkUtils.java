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
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A utility class for network.
 */
public class NetworkUtils {

  public static synchronized String getHostname() {
    InetAddress localAddress;
    try (DatagramSocket s = new DatagramSocket()) {
      // see https://stackoverflow.com/questions/9481865/getting-the-ip-address-of-the-current-machine-using-java
      // for details.
      s.connect(InetAddress.getByName("8.8.8.8"), 10002);
      localAddress = s.getLocalAddress();
      if (validAddress(localAddress)) {
        return localAddress.getHostAddress();
      }
    } catch (IOException e) {
      throw new HoodieException("Unable to find server port", e);
    }

    // fallback
    try {
      List<NetworkInterface> activeNetworkIFs = Collections.list(NetworkInterface.getNetworkInterfaces());
      // On unix-like system, getNetworkInterfaces returns ifs in reverse order
      // compared to ifconfig output order,
      // pick ip address following system output order.
      Collections.reverse(activeNetworkIFs);
      for (NetworkInterface ni : activeNetworkIFs) {
        List<InetAddress> addresses = Collections.list(ni.getInetAddresses()).stream()
            .filter(NetworkUtils::validAddress)
            .collect(Collectors.toList());
        if (addresses.size() > 0) {
          // IPv4 has higher priority
          InetAddress address = addresses.stream()
              .filter(addr -> addr instanceof Inet4Address).findAny()
              .orElse(addresses.get(0));
          try {
            // Inet6Address.toHostName may add interface at the end if it knows about it
            return InetAddress.getByAddress(address.getAddress()).getHostAddress();
          } catch (UnknownHostException e) {
            throw new HoodieException("Unable to fetch raw IP address for: " + address);
          }
        }
      }

      return localAddress.getHostAddress();
    } catch (SocketException e) {
      throw new HoodieException("Unable to find server port", e);
    }
  }

  private static boolean validAddress(InetAddress address) {
    return !(address.isLinkLocalAddress() || address.isLoopbackAddress() || address.isAnyLocalAddress());
  }
}
