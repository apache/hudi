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

package org.apache.hudi.hbase.zookeeper;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.hbase.HConstants;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility methods for reading, and building the ZooKeeper configuration.
 *
 * The order and priority for reading the config are as follows:
 * (1). Property with "hbase.zookeeper.property." prefix from HBase XML
 * (2). other zookeeper related properties in HBASE XML
 */
@InterfaceAudience.Private
public final class ZKConfig {

  private static final String VARIABLE_START = "${";

  private ZKConfig() {
  }

  /**
   * Make a Properties object holding ZooKeeper config.
   * Parses the corresponding config options from the HBase XML configs
   * and generates the appropriate ZooKeeper properties.
   * @param conf Configuration to read from.
   * @return Properties holding mappings representing ZooKeeper config file.
   */
  public static Properties makeZKProps(Configuration conf) {
    return makeZKPropsFromHbaseConfig(conf);
  }

  /**
   * Make a Properties object holding ZooKeeper config.
   * Parses the corresponding config options from the HBase XML configs
   * and generates the appropriate ZooKeeper properties.
   *
   * @param conf Configuration to read from.
   * @return Properties holding mappings representing ZooKeeper config file.
   */
  private static Properties makeZKPropsFromHbaseConfig(Configuration conf) {
    Properties zkProperties = new Properties();

    // Directly map all of the hbase.zookeeper.property.KEY properties.
    // Synchronize on conf so no loading of configs while we iterate
    synchronized (conf) {
      for (Entry<String, String> entry : conf) {
        String key = entry.getKey();
        if (key.startsWith(HConstants.ZK_CFG_PROPERTY_PREFIX)) {
          String zkKey = key.substring(HConstants.ZK_CFG_PROPERTY_PREFIX_LEN);
          String value = entry.getValue();
          // If the value has variables substitutions, need to do a get.
          if (value.contains(VARIABLE_START)) {
            value = conf.get(key);
          }
          zkProperties.setProperty(zkKey, value);
        }
      }
    }

    // If clientPort is not set, assign the default.
    if (zkProperties.getProperty(HConstants.CLIENT_PORT_STR) == null) {
      zkProperties.put(HConstants.CLIENT_PORT_STR,
          HConstants.DEFAULT_ZOOKEEPER_CLIENT_PORT);
    }

    // Create the server.X properties.
    int peerPort = conf.getInt("hbase.zookeeper.peerport", 2888);
    int leaderPort = conf.getInt("hbase.zookeeper.leaderport", 3888);

    final String[] serverHosts = conf.getStrings(HConstants.ZOOKEEPER_QUORUM,
        HConstants.LOCALHOST);
    String serverHost;
    String address;
    String key;
    for (int i = 0; i < serverHosts.length; ++i) {
      if (serverHosts[i].contains(":")) {
        serverHost = serverHosts[i].substring(0, serverHosts[i].indexOf(':'));
      } else {
        serverHost = serverHosts[i];
      }
      address = serverHost + ":" + peerPort + ":" + leaderPort;
      key = "server." + i;
      zkProperties.put(key, address);
    }

    return zkProperties;
  }

  /**
   * Return the ZK Quorum servers string given the specified configuration
   *
   * @param conf
   * @return Quorum servers String
   */
  private static String getZKQuorumServersStringFromHbaseConfig(Configuration conf) {
    String defaultClientPort = Integer.toString(
        conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, HConstants.DEFAULT_ZOOKEEPER_CLIENT_PORT));

    // Build the ZK quorum server string with "server:clientport" list, separated by ','
    final String[] serverHosts =
        conf.getStrings(HConstants.ZOOKEEPER_QUORUM, HConstants.LOCALHOST);
    return buildZKQuorumServerString(serverHosts, defaultClientPort);
  }

  /**
   * Return the ZK Quorum servers string given the specified configuration.
   * @return Quorum servers
   */
  public static String getZKQuorumServersString(Configuration conf) {
    return getZKQuorumServersStringFromHbaseConfig(conf);
  }

  /**
   * Build the ZK quorum server string with "server:clientport" list, separated by ','
   *
   * @param serverHosts a list of servers for ZK quorum
   * @param clientPort the default client port
   * @return the string for a list of "server:port" separated by ","
   */
  public static String buildZKQuorumServerString(String[] serverHosts, String clientPort) {
    StringBuilder quorumStringBuilder = new StringBuilder();
    String serverHost;
    for (int i = 0; i < serverHosts.length; ++i) {
      if (serverHosts[i].contains(":")) {
        serverHost = serverHosts[i]; // just use the port specified from the input
      } else {
        serverHost = serverHosts[i] + ":" + clientPort;
      }
      if (i > 0) {
        quorumStringBuilder.append(',');
      }
      quorumStringBuilder.append(serverHost);
    }
    return quorumStringBuilder.toString();
  }

  /**
   * Verifies that the given key matches the expected format for a ZooKeeper cluster key.
   * The Quorum for the ZK cluster can have one the following formats (see examples below):
   *
   * <ol>
   *   <li>s1,s2,s3 (no client port in the list, the client port could be obtained from
   *       clientPort)</li>
   *   <li>s1:p1,s2:p2,s3:p3 (with client port, which could be same or different for each server,
   *       in this case, the clientPort would be ignored)</li>
   *   <li>s1:p1,s2,s3:p3 (mix of (1) and (2) - if port is not specified in a server, it would use
   *       the clientPort; otherwise, it would use the specified port)</li>
   * </ol>
   *
   * @param key the cluster key to validate
   * @throws IOException if the key could not be parsed
   */
  public static void validateClusterKey(String key) throws IOException {
    transformClusterKey(key);
  }

  /**
   * Separate the given key into the three configurations it should contain:
   * hbase.zookeeper.quorum, hbase.zookeeper.client.port
   * and zookeeper.znode.parent
   * @param key
   * @return the three configuration in the described order
   * @throws IOException
   */
  public static ZKClusterKey transformClusterKey(String key) throws IOException {
    String[] parts = key.split(":");

    if (parts.length == 3) {
      if (!parts[2].matches("/.*[^/]")) {
        throw new IOException("Cluster key passed " + key + " is invalid, the format should be:" +
            HConstants.ZOOKEEPER_QUORUM + ":" + HConstants.ZOOKEEPER_CLIENT_PORT + ":"
            + HConstants.ZOOKEEPER_ZNODE_PARENT);
      }
      return new ZKClusterKey(parts [0], Integer.parseInt(parts [1]), parts [2]);
    }

    if (parts.length > 3) {
      // The quorum could contain client port in server:clientport format, try to transform more.
      String zNodeParent = parts [parts.length - 1];
      if (!zNodeParent.matches("/.*[^/]")) {
        throw new IOException("Cluster key passed " + key + " is invalid, the format should be:"
            + HConstants.ZOOKEEPER_QUORUM + ":" + HConstants.ZOOKEEPER_CLIENT_PORT + ":"
            + HConstants.ZOOKEEPER_ZNODE_PARENT);
      }

      String clientPort = parts [parts.length - 2];

      // The first part length is the total length minus the lengths of other parts and minus 2 ":"
      int endQuorumIndex = key.length() - zNodeParent.length() - clientPort.length() - 2;
      String quorumStringInput = key.substring(0, endQuorumIndex);
      String[] serverHosts = quorumStringInput.split(",");

      // The common case is that every server has its own client port specified - this means
      // that (total parts - the ZNodeParent part - the ClientPort part) is equal to
      // (the number of "," + 1) - "+ 1" because the last server has no ",".
      if ((parts.length - 2) == (serverHosts.length + 1)) {
        return new ZKClusterKey(quorumStringInput, Integer.parseInt(clientPort), zNodeParent);
      }

      // For the uncommon case that some servers has no port specified, we need to build the
      // server:clientport list using default client port for servers without specified port.
      return new ZKClusterKey(
          buildZKQuorumServerString(serverHosts, clientPort),
          Integer.parseInt(clientPort),
          zNodeParent);
    }

    throw new IOException("Cluster key passed " + key + " is invalid, the format should be:" +
        HConstants.ZOOKEEPER_QUORUM + ":" + HConstants.ZOOKEEPER_CLIENT_PORT + ":"
        + HConstants.ZOOKEEPER_ZNODE_PARENT);
  }

  /**
   * Get the key to the ZK ensemble for this configuration without
   * adding a name at the end
   * @param conf Configuration to use to build the key
   * @return ensemble key without a name
   */
  public static String getZooKeeperClusterKey(Configuration conf) {
    return getZooKeeperClusterKey(conf, null);
  }

  /**
   * Get the key to the ZK ensemble for this configuration and append
   * a name at the end
   * @param conf Configuration to use to build the key
   * @param name Name that should be appended at the end if not empty or null
   * @return ensemble key with a name (if any)
   */
  public static String getZooKeeperClusterKey(Configuration conf, String name) {
    String ensemble = conf.get(HConstants.ZOOKEEPER_QUORUM).replaceAll(
        "[\\t\\n\\x0B\\f\\r]", "");
    StringBuilder builder = new StringBuilder(ensemble);
    builder.append(":");
    builder.append(conf.get(HConstants.ZOOKEEPER_CLIENT_PORT));
    builder.append(":");
    builder.append(conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
    if (name != null && !name.isEmpty()) {
      builder.append(",");
      builder.append(name);
    }
    return builder.toString();
  }

  /**
   * Standardize the ZK quorum string: make it a "server:clientport" list, separated by ','
   * @param quorumStringInput a string contains a list of servers for ZK quorum
   * @param clientPort the default client port
   * @return the string for a list of "server:port" separated by ","
   */
  public static String standardizeZKQuorumServerString(String quorumStringInput,
                                                       String clientPort) {
    String[] serverHosts = quorumStringInput.split(",");
    return buildZKQuorumServerString(serverHosts, clientPort);
  }

  // The Quorum for the ZK cluster can have one the following format (see examples below):
  // (1). s1,s2,s3 (no client port in the list, the client port could be obtained from clientPort)
  // (2). s1:p1,s2:p2,s3:p3 (with client port, which could be same or different for each server,
  //      in this case, the clientPort would be ignored)
  // (3). s1:p1,s2,s3:p3 (mix of (1) and (2) - if port is not specified in a server, it would use
  //      the clientPort; otherwise, it would use the specified port)
  public static class ZKClusterKey {
    private String quorumString;
    private int clientPort;
    private String znodeParent;

    ZKClusterKey(String quorumString, int clientPort, String znodeParent) {
      this.quorumString = quorumString;
      this.clientPort = clientPort;
      this.znodeParent = znodeParent;
    }

    public String getQuorumString() {
      return quorumString;
    }

    public int getClientPort() {
      return clientPort;
    }

    public String getZnodeParent() {
      return znodeParent;
    }
  }

  /**
   * Get the client ZK Quorum servers string
   * @param conf the configuration to read
   * @return Client quorum servers, or null if not specified
   */
  public static String getClientZKQuorumServersString(Configuration conf) {
    String clientQuromServers = conf.get(HConstants.CLIENT_ZOOKEEPER_QUORUM);
    if (clientQuromServers == null) {
      return null;
    }
    int defaultClientPort =
        conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, HConstants.DEFAULT_ZOOKEEPER_CLIENT_PORT);
    String clientZkClientPort =
        Integer.toString(conf.getInt(HConstants.CLIENT_ZOOKEEPER_CLIENT_PORT, defaultClientPort));
    // Build the ZK quorum server string with "server:clientport" list, separated by ','
    final String[] serverHosts = StringUtils.getStrings(clientQuromServers);
    return buildZKQuorumServerString(serverHosts, clientZkClientPort);
  }
}
