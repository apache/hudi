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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.hbase.util.VersionInfo;
import org.apache.hudi.hbase.zookeeper.ZKConfig;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds HBase configuration files to a Configuration
 */
@InterfaceAudience.Public
public class HBaseConfiguration extends Configuration {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseConfiguration.class);

  /**
   * Instantiating HBaseConfiguration() is deprecated. Please use
   * HBaseConfiguration#create() to construct a plain Configuration
   * @deprecated since 0.90.0. Please use {@link #create()} instead.
   * @see #create()
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-2036">HBASE-2036</a>
   */
  @Deprecated
  public HBaseConfiguration() {
    //TODO:replace with private constructor, HBaseConfiguration should not extend Configuration
    super();
    addHbaseResources(this);
    LOG.warn("instantiating HBaseConfiguration() is deprecated. Please use"
        + " HBaseConfiguration#create() to construct a plain Configuration");
  }

  /**
   * Instantiating HBaseConfiguration() is deprecated. Please use
   * HBaseConfiguration#create(conf) to construct a plain Configuration
   * @deprecated since 0.90.0. Please use {@link #create(Configuration)} instead.
   * @see #create(Configuration)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-2036">HBASE-2036</a>
   */
  @Deprecated
  public HBaseConfiguration(final Configuration c) {
    //TODO:replace with private constructor
    this();
    merge(this, c);
  }

  private static void checkDefaultsVersion(Configuration conf) {
    if (conf.getBoolean("hbase.defaults.for.version.skip", Boolean.FALSE)) return;
    String defaultsVersion = conf.get("hbase.defaults.for.version");
    String thisVersion = VersionInfo.getVersion();
    if (!thisVersion.equals(defaultsVersion)) {
      throw new RuntimeException(
          "hbase-default.xml file seems to be for an older version of HBase (" +
              defaultsVersion + "), this version is " + thisVersion);
    }
  }

  public static Configuration addHbaseResources(Configuration conf) {
    conf.addResource("hbase-default.xml");
    conf.addResource("hbase-site.xml");

    checkDefaultsVersion(conf);
    return conf;
  }

  /**
   * Creates a Configuration with HBase resources
   * @return a Configuration with HBase resources
   */
  public static Configuration create() {
    Configuration conf = new Configuration();
    // In case HBaseConfiguration is loaded from a different classloader than
    // Configuration, conf needs to be set with appropriate class loader to resolve
    // HBase resources.
    conf.setClassLoader(HBaseConfiguration.class.getClassLoader());
    return addHbaseResources(conf);
  }

  /**
   * @param that Configuration to clone.
   * @return a Configuration created with the hbase-*.xml files plus
   * the given configuration.
   */
  public static Configuration create(final Configuration that) {
    Configuration conf = create();
    merge(conf, that);
    return conf;
  }

  /**
   * Merge two configurations.
   * @param destConf the configuration that will be overwritten with items
   *                 from the srcConf
   * @param srcConf the source configuration
   **/
  public static void merge(Configuration destConf, Configuration srcConf) {
    for (Map.Entry<String, String> e : srcConf) {
      destConf.set(e.getKey(), e.getValue());
    }
  }

  /**
   * Returns a subset of the configuration properties, matching the given key prefix.
   * The prefix is stripped from the return keys, ie. when calling with a prefix of "myprefix",
   * the entry "myprefix.key1 = value1" would be returned as "key1 = value1".  If an entry's
   * key matches the prefix exactly ("myprefix = value2"), it will <strong>not</strong> be
   * included in the results, since it would show up as an entry with an empty key.
   */
  public static Configuration subset(Configuration srcConf, String prefix) {
    Configuration newConf = new Configuration(false);
    for (Map.Entry<String, String> entry : srcConf) {
      if (entry.getKey().startsWith(prefix)) {
        String newKey = entry.getKey().substring(prefix.length());
        // avoid entries that would produce an empty key
        if (!newKey.isEmpty()) {
          newConf.set(newKey, entry.getValue());
        }
      }
    }
    return newConf;
  }

  /**
   * Sets all the entries in the provided {@code Map<String, String>} as properties in the
   * given {@code Configuration}.  Each property will have the specified prefix prepended,
   * so that the configuration entries are keyed by {@code prefix + entry.getKey()}.
   */
  public static void setWithPrefix(Configuration conf, String prefix,
                                   Iterable<Map.Entry<String, String>> properties) {
    for (Map.Entry<String, String> entry : properties) {
      conf.set(prefix + entry.getKey(), entry.getValue());
    }
  }

  /**
   * @return whether to show HBase Configuration in servlet
   */
  public static boolean isShowConfInServlet() {
    boolean isShowConf = false;
    try {
      if (Class.forName("org.apache.hadoop.conf.ConfServlet") != null) {
        isShowConf = true;
      }
    } catch (LinkageError e) {
      // should we handle it more aggressively in addition to log the error?
      LOG.warn("Error thrown: ", e);
    } catch (ClassNotFoundException ce) {
      LOG.debug("ClassNotFound: ConfServlet");
      // ignore
    }
    return isShowConf;
  }

  /**
   * Get the value of the <code>name</code> property as an <code>int</code>, possibly referring to
   * the deprecated name of the configuration property. If no such property exists, the provided
   * default value is returned, or if the specified value is not a valid <code>int</code>, then an
   * error is thrown.
   * @param name property name.
   * @param deprecatedName a deprecatedName for the property to use if non-deprecated name is not
   *          used
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as an <code>int</code>, or <code>defaultValue</code>.
   * @deprecated it will be removed in 3.0.0. Use
   *             {@link Configuration#addDeprecation(String, String)} instead.
   */
  @Deprecated
  public static int getInt(Configuration conf, String name,
                           String deprecatedName, int defaultValue) {
    if (conf.get(deprecatedName) != null) {
      LOG.warn(String.format("Config option \"%s\" is deprecated. Instead, use \"%s\""
          , deprecatedName, name));
      return conf.getInt(deprecatedName, defaultValue);
    } else {
      return conf.getInt(name, defaultValue);
    }
  }

  /**
   * Get the password from the Configuration instance using the
   * getPassword method if it exists. If not, then fall back to the
   * general get method for configuration elements.
   *
   * @param conf    configuration instance for accessing the passwords
   * @param alias   the name of the password element
   * @param defPass the default password
   * @return String password or default password
   * @throws IOException
   */
  public static String getPassword(Configuration conf, String alias,
                                   String defPass) throws IOException {
    String passwd = null;
    try {
      Method m = Configuration.class.getMethod("getPassword", String.class);
      char[] p = (char[]) m.invoke(conf, alias);
      if (p != null) {
        LOG.debug(String.format("Config option \"%s\" was found through" +
            " the Configuration getPassword method.", alias));
        passwd = new String(p);
      } else {
        LOG.debug(String.format(
            "Config option \"%s\" was not found. Using provided default value",
            alias));
        passwd = defPass;
      }
    } catch (NoSuchMethodException e) {
      // this is a version of Hadoop where the credential
      //provider API doesn't exist yet
      LOG.debug(String.format(
          "Credential.getPassword method is not available." +
              " Falling back to configuration."));
      passwd = conf.get(alias, defPass);
    } catch (SecurityException e) {
      throw new IOException(e.getMessage(), e);
    } catch (IllegalAccessException e) {
      throw new IOException(e.getMessage(), e);
    } catch (IllegalArgumentException e) {
      throw new IOException(e.getMessage(), e);
    } catch (InvocationTargetException e) {
      throw new IOException(e.getMessage(), e);
    }
    return passwd;
  }

  /**
   * Generates a {@link Configuration} instance by applying the ZooKeeper cluster key
   * to the base Configuration.  Note that additional configuration properties may be needed
   * for a remote cluster, so it is preferable to use
   * {@link #createClusterConf(Configuration, String, String)}.
   *
   * @param baseConf the base configuration to use, containing prefixed override properties
   * @param clusterKey the ZooKeeper quorum cluster key to apply, or {@code null} if none
   *
   * @return the merged configuration with override properties and cluster key applied
   *
   * @see #createClusterConf(Configuration, String, String)
   */
  public static Configuration createClusterConf(Configuration baseConf, String clusterKey)
      throws IOException {
    return createClusterConf(baseConf, clusterKey, null);
  }

  /**
   * Generates a {@link Configuration} instance by applying property overrides prefixed by
   * a cluster profile key to the base Configuration.  Override properties are extracted by
   * the {@link #subset(Configuration, String)} method, then the merged on top of the base
   * Configuration and returned.
   *
   * @param baseConf the base configuration to use, containing prefixed override properties
   * @param clusterKey the ZooKeeper quorum cluster key to apply, or {@code null} if none
   * @param overridePrefix the property key prefix to match for override properties,
   *     or {@code null} if none
   * @return the merged configuration with override properties and cluster key applied
   */
  public static Configuration createClusterConf(Configuration baseConf, String clusterKey,
                                                String overridePrefix) throws IOException {
    Configuration clusterConf = HBaseConfiguration.create(baseConf);
    if (clusterKey != null && !clusterKey.isEmpty()) {
      applyClusterKeyToConf(clusterConf, clusterKey);
    }

    if (overridePrefix != null && !overridePrefix.isEmpty()) {
      Configuration clusterSubset = HBaseConfiguration.subset(clusterConf, overridePrefix);
      HBaseConfiguration.merge(clusterConf, clusterSubset);
    }
    return clusterConf;
  }

  /**
   * Apply the settings in the given key to the given configuration, this is
   * used to communicate with distant clusters
   * @param conf configuration object to configure
   * @param key string that contains the 3 required configuratins
   */
  private static void applyClusterKeyToConf(Configuration conf, String key)
      throws IOException {
    ZKConfig.ZKClusterKey zkClusterKey = ZKConfig.transformClusterKey(key);
    conf.set(HConstants.ZOOKEEPER_QUORUM, zkClusterKey.getQuorumString());
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zkClusterKey.getClientPort());
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, zkClusterKey.getZnodeParent());
    // Without the right registry, the above configs are useless. Also, we don't use setClass()
    // here because the ConnectionRegistry* classes are not resolvable from this module.
    // This will be broken if ZkConnectionRegistry class gets renamed or moved. Is there a better
    // way?
    LOG.info("Overriding client registry implementation to {}",
        HConstants.ZK_CONNECTION_REGISTRY_CLASS);
    conf.set(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
        HConstants.ZK_CONNECTION_REGISTRY_CLASS);
  }

  /**
   * For debugging.  Dump configurations to system output as xml format.
   * Master and RS configurations can also be dumped using
   * http services. e.g. "curl http://master:16010/dump"
   */
  public static void main(String[] args) throws Exception {
    HBaseConfiguration.create().writeXml(System.out);
  }
}
