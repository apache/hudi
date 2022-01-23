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

package org.apache.hudi.hbase.util;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility for network addresses, resolving and naming.
 */
@InterfaceAudience.Private
public class Addressing {
  public static final String VALID_PORT_REGEX = "[\\d]+";
  public static final String HOSTNAME_PORT_SEPARATOR = ":";

  /**
   * @param hostAndPort Formatted as <code>&lt;hostname&gt; ':' &lt;port&gt;</code>
   * @return An InetSocketInstance
   */
  public static InetSocketAddress createInetSocketAddressFromHostAndPortStr(
      final String hostAndPort) {
    return new InetSocketAddress(parseHostname(hostAndPort), parsePort(hostAndPort));
  }

  /**
   * @param hostname Server hostname
   * @param port Server port
   * @return Returns a concatenation of <code>hostname</code> and
   * <code>port</code> in following
   * form: <code>&lt;hostname&gt; ':' &lt;port&gt;</code>.  For example, if hostname
   * is <code>example.org</code> and port is 1234, this method will return
   * <code>example.org:1234</code>
   */
  public static String createHostAndPortStr(final String hostname, final int port) {
    return hostname + HOSTNAME_PORT_SEPARATOR + port;
  }

  /**
   * @param hostAndPort Formatted as <code>&lt;hostname&gt; ':' &lt;port&gt;</code>
   * @return The hostname portion of <code>hostAndPort</code>
   */
  public static String parseHostname(final String hostAndPort) {
    int colonIndex = hostAndPort.lastIndexOf(HOSTNAME_PORT_SEPARATOR);
    if (colonIndex < 0) {
      throw new IllegalArgumentException("Not a host:port pair: " + hostAndPort);
    }
    return hostAndPort.substring(0, colonIndex);
  }

  /**
   * @param hostAndPort Formatted as <code>&lt;hostname&gt; ':' &lt;port&gt;</code>
   * @return The port portion of <code>hostAndPort</code>
   */
  public static int parsePort(final String hostAndPort) {
    int colonIndex = hostAndPort.lastIndexOf(HOSTNAME_PORT_SEPARATOR);
    if (colonIndex < 0) {
      throw new IllegalArgumentException("Not a host:port pair: " + hostAndPort);
    }
    return Integer.parseInt(hostAndPort.substring(colonIndex + 1));
  }

  public static InetAddress getIpAddress() throws SocketException {
    return getIpAddress(new AddressSelectionCondition() {
      @Override
      public boolean isAcceptableAddress(InetAddress addr) {
        return addr instanceof Inet4Address || addr instanceof Inet6Address;
      }
    });
  }

  public static InetAddress getIp4Address() throws SocketException {
    return getIpAddress(new AddressSelectionCondition() {
      @Override
      public boolean isAcceptableAddress(InetAddress addr) {
        return addr instanceof Inet4Address;
      }
    });
  }

  public static InetAddress getIp6Address() throws SocketException {
    return getIpAddress(new AddressSelectionCondition() {
      @Override
      public boolean isAcceptableAddress(InetAddress addr) {
        return addr instanceof Inet6Address;
      }
    });
  }

  private static InetAddress getIpAddress(AddressSelectionCondition condition) throws
      SocketException {
    // Before we connect somewhere, we cannot be sure about what we'd be bound to; however,
    // we only connect when the message where client ID is, is long constructed. Thus,
    // just use whichever IP address we can find.
    Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
    while (interfaces.hasMoreElements()) {
      NetworkInterface current = interfaces.nextElement();
      if (!current.isUp() || current.isLoopback() || current.isVirtual()) continue;
      Enumeration<InetAddress> addresses = current.getInetAddresses();
      while (addresses.hasMoreElements()) {
        InetAddress addr = addresses.nextElement();
        if (addr.isLoopbackAddress()) continue;
        if (condition.isAcceptableAddress(addr)) {
          return addr;
        }
      }
    }

    throw new SocketException("Can't get our ip address, interfaces are: " + interfaces);
  }

  /**
   * Given an InetAddress, checks to see if the address is a local address, by comparing the address
   * with all the interfaces on the node.
   * @param addr address to check if it is local node's address
   * @return true if the address corresponds to the local node
   */
  public static boolean isLocalAddress(InetAddress addr) {
    // Check if the address is any local or loop back
    boolean local = addr.isAnyLocalAddress() || addr.isLoopbackAddress();

    // Check if the address is defined on any interface
    if (!local) {
      try {
        local = NetworkInterface.getByInetAddress(addr) != null;
      } catch (SocketException e) {
        local = false;
      }
    }
    return local;
  }

  /**
   * Given an InetSocketAddress object returns a String represent of it.
   * This is a util method for Java 17. The toString() function of InetSocketAddress
   * will flag the unresolved address with a substring in the string, which will result
   * in unexpected problem. We should use this util function to get the string when we
   * not sure whether the input address is resolved or not.
   * @param address address to convert to a "host:port" String.
   * @return the String represent of the given address, like "foo:1234".
   */
  public static String inetSocketAddress2String(InetSocketAddress address) {
    return address.isUnresolved() ?
        address.toString().replace("/<unresolved>", "") :
        address.toString();
  }

  /**
   * Interface for AddressSelectionCondition to check if address is acceptable
   */
  public interface AddressSelectionCondition{
    /**
     * Condition on which to accept inet address
     * @param address to check
     * @return true to accept this address
     */
    public boolean isAcceptableAddress(InetAddress address);
  }
}
