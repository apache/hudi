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

package org.apache.hudi.hbase;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.apache.hudi.hbase.net.Address;
import org.apache.hudi.hbase.util.Addressing;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.collect.Interner;
import org.apache.hbase.thirdparty.com.google.common.collect.Interners;
import org.apache.hbase.thirdparty.com.google.common.net.InetAddresses;

/**
 * Name of a particular incarnation of an HBase Server.
 * A {@link ServerName} is used uniquely identifying a server instance in a cluster and is made
 * of the combination of hostname, port, and startcode.  The startcode distinguishes restarted
 * servers on same hostname and port (startcode is usually timestamp of server startup). The
 * {@link #toString()} format of ServerName is safe to use in the  filesystem and as znode name
 * up in ZooKeeper.  Its format is:
 * <code>&lt;hostname&gt; '{@link #SERVERNAME_SEPARATOR}' &lt;port&gt;
 * '{@link #SERVERNAME_SEPARATOR}' &lt;startcode&gt;</code>.
 * For example, if hostname is <code>www.example.org</code>, port is <code>1234</code>,
 * and the startcode for the regionserver is <code>1212121212</code>, then
 * the {@link #toString()} would be <code>www.example.org,1234,1212121212</code>.
 *
 * <p>You can obtain a versioned serialized form of this class by calling
 * {@link #getVersionedBytes()}.  To deserialize, call
 * {@link #parseVersionedServerName(byte[])}.
 *
 * <p>Use {@link #getAddress()} to obtain the Server hostname + port
 * (Endpoint/Socket Address).
 *
 * <p>Immutable.
 */
@InterfaceAudience.Public
public class ServerName implements Comparable<ServerName>, Serializable {
  private static final long serialVersionUID = 1367463982557264981L;

  /**
   * Version for this class.
   * Its a short rather than a byte so I can for sure distinguish between this
   * version of this class and the version previous to this which did not have
   * a version.
   */
  private static final short VERSION = 0;
  static final byte [] VERSION_BYTES = Bytes.toBytes(VERSION);

  /**
   * What to use if no startcode supplied.
   */
  public static final int NON_STARTCODE = -1;

  /**
   * This character is used as separator between server hostname, port and
   * startcode.
   */
  public static final String SERVERNAME_SEPARATOR = ",";

  public static final Pattern SERVERNAME_PATTERN =
      Pattern.compile("[^" + SERVERNAME_SEPARATOR + "]+" +
          SERVERNAME_SEPARATOR + Addressing.VALID_PORT_REGEX +
          SERVERNAME_SEPARATOR + Addressing.VALID_PORT_REGEX + "$");

  /**
   * What to use if server name is unknown.
   */
  public static final String UNKNOWN_SERVERNAME = "#unknown#";

  private final String servername;
  private final long startcode;
  private transient Address address;

  /**
   * Cached versioned bytes of this ServerName instance.
   * @see #getVersionedBytes()
   */
  private byte [] bytes;
  public static final List<ServerName> EMPTY_SERVER_LIST = new ArrayList<>(0);

  /**
   * Intern ServerNames. The Set of ServerNames is mostly-fixed changing slowly as Servers
   * restart. Rather than create a new instance everytime, try and return existing instance
   * if there is one.
   */
  private static final Interner<ServerName> INTERN_POOL = Interners.newWeakInterner();

  protected ServerName(final String hostname, final int port, final long startcode) {
    this(Address.fromParts(hostname, port), startcode);
  }

  private ServerName(final Address address, final long startcode) {
    // Use HostAndPort to host port and hostname. Does validation and can do ipv6
    this.address = address;
    this.startcode = startcode;
    this.servername = getServerName(this.address.getHostname(),
        this.address.getPort(), startcode);
  }

  private ServerName(final String hostAndPort, final long startCode) {
    this(Address.fromString(hostAndPort), startCode);
  }

  /**
   * @param hostname the hostname string to get the actual hostname from
   * @return hostname minus the domain, if there is one (will do pass-through on ip addresses)
   * @deprecated Since 2.0. This is for internal use only.
   */
  @Deprecated
  // Make this private in hbase-3.0.
  static String getHostNameMinusDomain(final String hostname) {
    if (InetAddresses.isInetAddress(hostname)) {
      return hostname;
    }
    String[] parts = hostname.split("\\.");
    if (parts.length == 0) {
      return hostname;
    }
    return parts[0];
  }

  /**
   * @deprecated Since 2.0. Use {@link #valueOf(String)}
   */
  @Deprecated
  // This is unused. Get rid of it.
  public static String parseHostname(final String serverName) {
    if (serverName == null || serverName.length() <= 0) {
      throw new IllegalArgumentException("Passed hostname is null or empty");
    }
    if (!Character.isLetterOrDigit(serverName.charAt(0))) {
      throw new IllegalArgumentException("Bad passed hostname, serverName=" + serverName);
    }
    int index = serverName.indexOf(SERVERNAME_SEPARATOR);
    return serverName.substring(0, index);
  }

  /**
   * @deprecated Since 2.0. Use {@link #valueOf(String)}
   */
  @Deprecated
  // This is unused. Get rid of it.
  public static int parsePort(final String serverName) {
    String [] split = serverName.split(SERVERNAME_SEPARATOR);
    return Integer.parseInt(split[1]);
  }

  /**
   * @deprecated Since 2.0. Use {@link #valueOf(String)}
   */
  @Deprecated
  // This is unused. Get rid of it.
  public static long parseStartcode(final String serverName) {
    int index = serverName.lastIndexOf(SERVERNAME_SEPARATOR);
    return Long.parseLong(serverName.substring(index + 1));
  }

  /**
   * Retrieve an instance of ServerName.
   * Callers should use the equals method to compare returned instances, though we may return
   * a shared immutable object as an internal optimization.
   */
  public static ServerName valueOf(final String hostname, final int port, final long startcode) {
    return INTERN_POOL.intern(new ServerName(hostname, port, startcode));
  }

  /**
   * Retrieve an instance of ServerName.
   * Callers should use the equals method to compare returned instances, though we may return
   * a shared immutable object as an internal optimization.
   */
  public static ServerName valueOf(final String serverName) {
    final String hostname = serverName.substring(0, serverName.indexOf(SERVERNAME_SEPARATOR));
    final int port = Integer.parseInt(serverName.split(SERVERNAME_SEPARATOR)[1]);
    final long statuscode =
        Long.parseLong(serverName.substring(serverName.lastIndexOf(SERVERNAME_SEPARATOR) + 1));
    return INTERN_POOL.intern(new ServerName(hostname, port, statuscode));
  }

  /**
   * Retrieve an instance of ServerName.
   * Callers should use the equals method to compare returned instances, though we may return
   * a shared immutable object as an internal optimization.
   */
  public static ServerName valueOf(final String hostAndPort, final long startCode) {
    return INTERN_POOL.intern(new ServerName(hostAndPort, startCode));
  }

  /**
   * Retrieve an instance of {@link ServerName}. Callers should use the {@link #equals(Object)}
   * method to compare returned instances, though we may return a shared immutable object as an
   * internal optimization.
   *
   * @param address the {@link Address} to use for getting the {@link ServerName}
   * @param startcode the startcode to use for getting the {@link ServerName}
   * @return the constructed {@link ServerName}
   * @see #valueOf(String, int, long)
   */
  public static ServerName valueOf(final Address address, final long startcode) {
    return valueOf(address.getHostname(), address.getPort(), startcode);
  }

  @Override
  public String toString() {
    return getServerName();
  }

  /**
   * @return Return a SHORT version of {@link #toString()}, one that has the host only,
   *   minus the domain, and the port only -- no start code; the String is for us internally mostly
   *   tying threads to their server.  Not for external use.  It is lossy and will not work in
   *   in compares, etc.
   */
  public String toShortString() {
    return Addressing.createHostAndPortStr(
        getHostNameMinusDomain(this.address.getHostname()),
        this.address.getPort());
  }

  /**
   * @return {@link #getServerName()} as bytes with a short-sized prefix with
   *   the {@link #VERSION} of this class.
   */
  public synchronized byte [] getVersionedBytes() {
    if (this.bytes == null) {
      this.bytes = Bytes.add(VERSION_BYTES, Bytes.toBytes(getServerName()));
    }
    return this.bytes;
  }

  public String getServerName() {
    return servername;
  }

  public String getHostname() {
    return this.address.getHostname();
  }

  public String getHostnameLowerCase() {
    return this.address.getHostname().toLowerCase(Locale.ROOT);
  }

  public int getPort() {
    return this.address.getPort();
  }

  public long getStartcode() {
    return startcode;
  }

  /**
   * For internal use only.
   * @param hostName the name of the host to use
   * @param port the port on the host to use
   * @param startcode the startcode to use for formatting
   * @return Server name made of the concatenation of hostname, port and
   *   startcode formatted as <code>&lt;hostname&gt; ',' &lt;port&gt; ',' &lt;startcode&gt;</code>
   * @deprecated Since 2.0. Use {@link ServerName#valueOf(String, int, long)} instead.
   */
  @Deprecated
  // TODO: Make this private in hbase-3.0.
  static String getServerName(String hostName, int port, long startcode) {
    return hostName.toLowerCase(Locale.ROOT) + SERVERNAME_SEPARATOR + port
        + SERVERNAME_SEPARATOR + startcode;
  }

  /**
   * @param hostAndPort String in form of &lt;hostname&gt; ':' &lt;port&gt;
   * @param startcode the startcode to use
   * @return Server name made of the concatenation of hostname, port and
   *   startcode formatted as <code>&lt;hostname&gt; ',' &lt;port&gt; ',' &lt;startcode&gt;</code>
   * @deprecated Since 2.0. Use {@link ServerName#valueOf(String, long)} instead.
   */
  @Deprecated
  public static String getServerName(final String hostAndPort, final long startcode) {
    int index = hostAndPort.indexOf(':');
    if (index <= 0) {
      throw new IllegalArgumentException("Expected <hostname> ':' <port>");
    }
    return getServerName(hostAndPort.substring(0, index),
        Integer.parseInt(hostAndPort.substring(index + 1)), startcode);
  }

  /**
   * @return Hostname and port formatted as described at
   * {@link Addressing#createHostAndPortStr(String, int)}
   * @deprecated Since 2.0. Use {@link #getAddress()} instead.
   */
  @Deprecated
  public String getHostAndPort() {
    return this.address.toString();
  }

  public Address getAddress() {
    return this.address;
  }

  /**
   * @param serverName ServerName in form specified by {@link #getServerName()}
   * @return The server start code parsed from <code>servername</code>
   * @deprecated Since 2.0. Use instance of ServerName to pull out start code.
   */
  @Deprecated
  public static long getServerStartcodeFromServerName(final String serverName) {
    int index = serverName.lastIndexOf(SERVERNAME_SEPARATOR);
    return Long.parseLong(serverName.substring(index + 1));
  }

  /**
   * Utility method to excise the start code from a server name
   * @param inServerName full server name
   * @return server name less its start code
   * @deprecated Since 2.0. Use {@link #getAddress()}
   */
  @Deprecated
  public static String getServerNameLessStartCode(String inServerName) {
    if (inServerName != null && inServerName.length() > 0) {
      int index = inServerName.lastIndexOf(SERVERNAME_SEPARATOR);
      if (index > 0) {
        return inServerName.substring(0, index);
      }
    }
    return inServerName;
  }

  @Override
  public int compareTo(ServerName other) {
    int compare;
    if (other == null) {
      return -1;
    }
    if (this.getHostname() == null) {
      if (other.getHostname() != null) {
        return 1;
      }
    } else {
      if (other.getHostname() == null) {
        return -1;
      }
      compare = this.getHostname().compareToIgnoreCase(other.getHostname());
      if (compare != 0) {
        return compare;
      }
    }
    compare = this.getPort() - other.getPort();
    if (compare != 0) {
      return compare;
    }
    return Long.compare(this.getStartcode(), other.getStartcode());
  }

  @Override
  public int hashCode() {
    return getServerName().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (!(o instanceof ServerName)) {
      return false;
    }
    return this.compareTo((ServerName)o) == 0;
  }

  /**
   * @param left the first server address to compare
   * @param right the second server address to compare
   * @return {@code true} if {@code left} and {@code right} have the same hostname and port.
   */
  public static boolean isSameAddress(final ServerName left, final ServerName right) {
    return left.getAddress().equals(right.getAddress());
  }

  /**
   * Use this method instantiating a {@link ServerName} from bytes
   * gotten from a call to {@link #getVersionedBytes()}.  Will take care of the
   * case where bytes were written by an earlier version of hbase.
   * @param versionedBytes Pass bytes gotten from a call to {@link #getVersionedBytes()}
   * @return A ServerName instance.
   * @see #getVersionedBytes()
   */
  public static ServerName parseVersionedServerName(final byte [] versionedBytes) {
    // Version is a short.
    short version = Bytes.toShort(versionedBytes);
    if (version == VERSION) {
      int length = versionedBytes.length - Bytes.SIZEOF_SHORT;
      return valueOf(Bytes.toString(versionedBytes, Bytes.SIZEOF_SHORT, length));
    }
    // Presume the bytes were written with an old version of hbase and that the
    // bytes are actually a String of the form "'<hostname>' ':' '<port>'".
    return valueOf(Bytes.toString(versionedBytes), NON_STARTCODE);
  }

  /**
   * @param str Either an instance of {@link #toString()} or a
   *   "'&lt;hostname&gt;' ':' '&lt;port&gt;'".
   * @return A ServerName instance.
   */
  public static ServerName parseServerName(final String str) {
    return SERVERNAME_PATTERN.matcher(str).matches()? valueOf(str) :
        valueOf(str, NON_STARTCODE);
  }

  /**
   * @return true if the String follows the pattern of {@link #toString()}, false
   *   otherwise.
   */
  public static boolean isFullServerName(final String str){
    if (str == null ||str.isEmpty()) {
      return false;
    }
    return SERVERNAME_PATTERN.matcher(str).matches();
  }
}
