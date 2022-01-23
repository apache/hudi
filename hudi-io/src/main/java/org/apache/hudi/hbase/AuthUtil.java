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
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.hbase.security.User;
import org.apache.hudi.hbase.security.UserProvider;
import org.apache.hudi.hbase.util.DNS;
import org.apache.hudi.hbase.util.Strings;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for helping with security tasks. Downstream users
 * may rely on this class to handle authenticating via keytab where
 * long running services need access to a secure HBase cluster.
 *
 * Callers must ensure:
 *
 * <ul>
 *   <li>HBase configuration files are in the Classpath
 *   <li>hbase.client.keytab.file points to a valid keytab on the local filesystem
 *   <li>hbase.client.kerberos.principal gives the Kerberos principal to use
 * </ul>
 *
 * <pre>
 * {@code
 *   ChoreService choreService = null;
 *   // Presumes HBase configuration files are on the classpath
 *   final Configuration conf = HBaseConfiguration.create();
 *   final ScheduledChore authChore = AuthUtil.getAuthChore(conf);
 *   if (authChore != null) {
 *     choreService = new ChoreService("MY_APPLICATION");
 *     choreService.scheduleChore(authChore);
 *   }
 *   try {
 *     // do application work
 *   } finally {
 *     if (choreService != null) {
 *       choreService.shutdown();
 *     }
 *   }
 * }
 * </pre>
 *
 * See the "Running Canary in a Kerberos-enabled Cluster" section of the HBase Reference Guide for
 * an example of configuring a user of this Auth Chore to run on a secure cluster.
 * <pre>
 * </pre>
 * This class will be internal used only from 2.2.0 version, and will transparently work
 * for kerberized applications. For more, please refer
 * <a href="http://hbase.apache.org/book.html#hbase.secure.configuration">Client-side Configuration for Secure Operation</a>
 *
 * @deprecated since 2.2.0, to be marked as
 *  {@link org.apache.yetus.audience.InterfaceAudience.Private} in 4.0.0.
 * @see <a href="https://issues.apache.org/jira/browse/HBASE-20886">HBASE-20886</a>
 */
@Deprecated
@InterfaceAudience.Public
public final class AuthUtil {
  private static final Logger LOG = LoggerFactory.getLogger(AuthUtil.class);

  /** Prefix character to denote group names */
  private static final String GROUP_PREFIX = "@";

  /** Client keytab file */
  public static final String HBASE_CLIENT_KEYTAB_FILE = "hbase.client.keytab.file";

  /** Client principal */
  public static final String HBASE_CLIENT_KERBEROS_PRINCIPAL = "hbase.client.keytab.principal";

  private AuthUtil() {
    super();
  }

  /**
   * For kerberized cluster, return login user (from kinit or from keytab if specified).
   * For non-kerberized cluster, return system user.
   * @param conf configuartion file
   * @return user
   * @throws IOException login exception
   */
  @InterfaceAudience.Private
  public static User loginClient(Configuration conf) throws IOException {
    UserProvider provider = UserProvider.instantiate(conf);
    User user = provider.getCurrent();
    boolean securityOn = provider.isHBaseSecurityEnabled() && provider.isHadoopSecurityEnabled();

    if (securityOn) {
      boolean fromKeytab = provider.shouldLoginFromKeytab();
      if (user.getUGI().hasKerberosCredentials()) {
        // There's already a login user.
        // But we should avoid misuse credentials which is a dangerous security issue,
        // so here check whether user specified a keytab and a principal:
        // 1. Yes, check if user principal match.
        //    a. match, just return.
        //    b. mismatch, login using keytab.
        // 2. No, user may login through kinit, this is the old way, also just return.
        if (fromKeytab) {
          return checkPrincipalMatch(conf, user.getUGI().getUserName()) ? user :
              loginFromKeytabAndReturnUser(provider);
        }
        return user;
      } else if (fromKeytab) {
        // Kerberos is on and client specify a keytab and principal, but client doesn't login yet.
        return loginFromKeytabAndReturnUser(provider);
      }
    }
    return user;
  }

  private static boolean checkPrincipalMatch(Configuration conf, String loginUserName) {
    String configuredUserName = conf.get(HBASE_CLIENT_KERBEROS_PRINCIPAL);
    boolean match = configuredUserName.equals(loginUserName);
    if (!match) {
      LOG.warn("Trying to login with a different user: {}, existed user is {}.",
          configuredUserName, loginUserName);
    }
    return match;
  }

  private static User loginFromKeytabAndReturnUser(UserProvider provider) throws IOException {
    try {
      provider.login(HBASE_CLIENT_KEYTAB_FILE, HBASE_CLIENT_KERBEROS_PRINCIPAL);
    } catch (IOException ioe) {
      LOG.error("Error while trying to login as user {} through {}, with message: {}.",
          HBASE_CLIENT_KERBEROS_PRINCIPAL, HBASE_CLIENT_KEYTAB_FILE,
          ioe.getMessage());
      throw ioe;
    }
    return provider.getCurrent();
  }

  /**
   * For kerberized cluster, return login user (from kinit or from keytab).
   * Principal should be the following format: name/fully.qualified.domain.name@REALM.
   * For non-kerberized cluster, return system user.
   * <p>
   * NOT recommend to use to method unless you're sure what you're doing, it is for canary only.
   * Please use User#loginClient.
   * @param conf configuration file
   * @return user
   * @throws IOException login exception
   */
  private static User loginClientAsService(Configuration conf) throws IOException {
    UserProvider provider = UserProvider.instantiate(conf);
    if (provider.isHBaseSecurityEnabled() && provider.isHadoopSecurityEnabled()) {
      try {
        if (provider.shouldLoginFromKeytab()) {
          String host = Strings.domainNamePointerToHostName(DNS.getDefaultHost(
              conf.get("hbase.client.dns.interface", "default"),
              conf.get("hbase.client.dns.nameserver", "default")));
          provider.login(HBASE_CLIENT_KEYTAB_FILE, HBASE_CLIENT_KERBEROS_PRINCIPAL, host);
        }
      } catch (UnknownHostException e) {
        LOG.error("Error resolving host name: " + e.getMessage(), e);
        throw e;
      } catch (IOException e) {
        LOG.error("Error while trying to perform the initial login: " + e.getMessage(), e);
        throw e;
      }
    }
    return provider.getCurrent();
  }

  /**
   * Checks if security is enabled and if so, launches chore for refreshing kerberos ticket.
   * @return a ScheduledChore for renewals.
   */
  @InterfaceAudience.Private
  public static ScheduledChore getAuthRenewalChore(final UserGroupInformation user) {
    if (!user.hasKerberosCredentials()) {
      return null;
    }

    Stoppable stoppable = createDummyStoppable();
    // if you're in debug mode this is useful to avoid getting spammed by the getTGT()
    // you can increase this, keeping in mind that the default refresh window is 0.8
    // e.g. 5min tgt * 0.8 = 4min refresh so interval is better be way less than 1min
    final int CHECK_TGT_INTERVAL = 30 * 1000; // 30sec
    return new ScheduledChore("RefreshCredentials", stoppable, CHECK_TGT_INTERVAL) {
      @Override
      protected void chore() {
        try {
          user.checkTGTAndReloginFromKeytab();
        } catch (IOException e) {
          LOG.error("Got exception while trying to refresh credentials: " + e.getMessage(), e);
        }
      }
    };
  }

  /**
   * Checks if security is enabled and if so, launches chore for refreshing kerberos ticket.
   * @param conf the hbase service configuration
   * @return a ScheduledChore for renewals, if needed, and null otherwise.
   * @deprecated Deprecated since 2.2.0, this method will be
   *   {@link org.apache.yetus.audience.InterfaceAudience.Private} use only after 4.0.0.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-20886">HBASE-20886</a>
   */
  @Deprecated
  public static ScheduledChore getAuthChore(Configuration conf) throws IOException {
    User user = loginClientAsService(conf);
    return getAuthRenewalChore(user.getUGI());
  }

  private static Stoppable createDummyStoppable() {
    return new Stoppable() {
      private volatile boolean isStopped = false;

      @Override
      public void stop(String why) {
        isStopped = true;
      }

      @Override
      public boolean isStopped() {
        return isStopped;
      }
    };
  }

  /**
   * Returns whether or not the given name should be interpreted as a group
   * principal.  Currently this simply checks if the name starts with the
   * special group prefix character ("@").
   */
  @InterfaceAudience.Private
  public static boolean isGroupPrincipal(String name) {
    return name != null && name.startsWith(GROUP_PREFIX);
  }

  /**
   * Returns the actual name for a group principal (stripped of the
   * group prefix).
   */
  @InterfaceAudience.Private
  public static String getGroupName(String aclKey) {
    if (!isGroupPrincipal(aclKey)) {
      return aclKey;
    }

    return aclKey.substring(GROUP_PREFIX.length());
  }

  /**
   * Returns the group entry with the group prefix for a group principal.
   */
  @InterfaceAudience.Private
  public static String toGroupEntry(String name) {
    return GROUP_PREFIX + name;
  }
}
