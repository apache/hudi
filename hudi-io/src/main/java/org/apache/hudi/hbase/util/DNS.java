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

import java.lang.reflect.Method;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Wrapper around Hadoop's DNS class to hide reflection.
 */
@InterfaceAudience.Private
public final class DNS {
  // key to the config parameter of server hostname
  // the specification of server hostname is optional. The hostname should be resolvable from
  // both master and region server
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
  public static final String UNSAFE_RS_HOSTNAME_KEY = "hbase.unsafe.regionserver.hostname";
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
  public static final String MASTER_HOSTNAME_KEY = "hbase.master.hostname";

  private static boolean HAS_NEW_DNS_GET_DEFAULT_HOST_API;
  private static Method GET_DEFAULT_HOST_METHOD;

  /**
   * @deprecated since 2.4.0 and will be removed in 4.0.0.
   * Use {@link DNS#UNSAFE_RS_HOSTNAME_KEY} instead.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-24667">HBASE-24667</a>
   */
  @Deprecated
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
  public static final String RS_HOSTNAME_KEY = "hbase.regionserver.hostname";

  static {
    try {
      GET_DEFAULT_HOST_METHOD = org.apache.hadoop.net.DNS.class
          .getMethod("getDefaultHost", String.class, String.class, boolean.class);
      HAS_NEW_DNS_GET_DEFAULT_HOST_API = true;
    } catch (Exception e) {
      HAS_NEW_DNS_GET_DEFAULT_HOST_API = false; // FindBugs: Causes REC_CATCH_EXCEPTION. Suppressed
    }
    Configuration.addDeprecation(RS_HOSTNAME_KEY, UNSAFE_RS_HOSTNAME_KEY);
  }

  public enum ServerType {
    MASTER("master"),
    REGIONSERVER("regionserver");

    private String name;
    ServerType(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  private DNS() {}

  /**
   * Wrapper around DNS.getDefaultHost(String, String), calling
   * DNS.getDefaultHost(String, String, boolean) when available.
   *
   * @param strInterface The network interface to query.
   * @param nameserver The DNS host name.
   * @return The default host names associated with IPs bound to the network interface.
   */
  public static String getDefaultHost(String strInterface, String nameserver)
      throws UnknownHostException {
    if (HAS_NEW_DNS_GET_DEFAULT_HOST_API) {
      try {
        // Hadoop-2.8 includes a String, String, boolean variant of getDefaultHost
        // which properly handles multi-homed systems with Kerberos.
        return (String) GET_DEFAULT_HOST_METHOD.invoke(null, strInterface, nameserver, true);
      } catch (Exception e) {
        // If we can't invoke the method as it should exist, throw an exception
        throw new RuntimeException("Failed to invoke DNS.getDefaultHost via reflection", e);
      }
    } else {
      return org.apache.hadoop.net.DNS.getDefaultHost(strInterface, nameserver);
    }
  }

  /**
   * Get the configured hostname for a given ServerType. Gets the default hostname if not specified
   * in the configuration.
   * @param conf Configuration to look up.
   * @param serverType ServerType to look up in the configuration for overrides.
   */
  public static String getHostname(Configuration conf, ServerType serverType)
      throws UnknownHostException {
    String hostname;
    switch (serverType) {
      case MASTER:
        hostname = conf.get(MASTER_HOSTNAME_KEY);
        break;
      case REGIONSERVER:
        hostname = conf.get(UNSAFE_RS_HOSTNAME_KEY);
        break;
      default:
        hostname = null;
    }
    if (hostname == null || hostname.isEmpty()) {
      return Strings.domainNamePointerToHostName(getDefaultHost(
          conf.get("hbase." + serverType.getName() + ".dns.interface", "default"),
          conf.get("hbase." + serverType.getName() + ".dns.nameserver", "default")));
    } else {
      return hostname;
    }
  }
}
