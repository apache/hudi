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

import java.io.PrintStream;
import java.io.PrintWriter;

import org.apache.commons.lang3.StringUtils;
import org.apache.hudi.hbase.Version;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class finds the Version information for HBase.
 */
@InterfaceAudience.Public
public class VersionInfo {
  private static final Logger LOG = LoggerFactory.getLogger(VersionInfo.class.getName());

  // If between two dots there is not a number, we regard it as a very large number so it is
  // higher than any numbers in the version.
  private static final int VERY_LARGE_NUMBER = 100000;

  /**
   * Get the hbase version.
   * @return the hbase version string, eg. "0.6.3-dev"
   */
  public static String getVersion() {
    return Version.version;
  }

  /**
   * Get the subversion revision number for the root directory
   * @return the revision number, eg. "451451"
   */
  public static String getRevision() {
    return Version.revision;
  }

  /**
   * The date that hbase was compiled.
   * @return the compilation date in unix date format
   */
  public static String getDate() {
    return Version.date;
  }

  /**
   * The user that compiled hbase.
   * @return the username of the user
   */
  public static String getUser() {
    return Version.user;
  }

  /**
   * Get the subversion URL for the root hbase directory.
   * @return the url
   */
  public static String getUrl() {
    return Version.url;
  }

  static String[] versionReport() {
    return new String[] {
        "HBase " + getVersion(),
        "Source code repository " + getUrl() + " revision=" + getRevision(),
        "Compiled by " + getUser() + " on " + getDate(),
        "From source with checksum " + getSrcChecksum()
    };
  }

  /**
   * Get the checksum of the source files from which Hadoop was compiled.
   * @return a string that uniquely identifies the source
   **/
  public static String getSrcChecksum() {
    return Version.srcChecksum;
  }

  public static void writeTo(PrintWriter out) {
    for (String line : versionReport()) {
      out.println(line);
    }
  }

  public static void writeTo(PrintStream out) {
    for (String line : versionReport()) {
      out.println(line);
    }
  }

  public static void logVersion() {
    for (String line : versionReport()) {
      LOG.info(line);
    }
  }

  public static int compareVersion(String v1, String v2) {
    //fast compare equals first
    if (v1.equals(v2)) {
      return 0;
    }
    String[] v1Comps = getVersionComponents(v1);
    String[] v2Comps = getVersionComponents(v2);

    int length = Math.max(v1Comps.length, v2Comps.length);
    for (int i = 0; i < length; i++) {
      Integer va = i < v1Comps.length ? Integer.parseInt(v1Comps[i]) : 0;
      Integer vb = i < v2Comps.length ? Integer.parseInt(v2Comps[i]) : 0;
      int compare = va.compareTo(vb);
      if (compare != 0) {
        return compare;
      }
    }
    return 0;
  }

  /**
   * Returns the version components as String objects
   * Examples: "1.2.3" returns ["1", "2", "3"], "4.5.6-SNAPSHOT" returns ["4", "5", "6", "-1"]
   * "4.5.6-beta" returns ["4", "5", "6", "-2"], "4.5.6-alpha" returns ["4", "5", "6", "-3"]
   * "4.5.6-UNKNOW" returns ["4", "5", "6", "-4"]
   * @return the components of the version string
   */
  private static String[] getVersionComponents(final String version) {
    assert(version != null);
    String[] strComps = version.split("[\\.-]");
    assert(strComps.length > 0);

    String[] comps = new String[strComps.length];
    for (int i = 0; i < strComps.length; ++i) {
      if (StringUtils.isNumeric(strComps[i])) {
        comps[i] = strComps[i];
      } else if (StringUtils.isEmpty(strComps[i])) {
        comps[i] = String.valueOf(VERY_LARGE_NUMBER);
      } else {
        if("SNAPSHOT".equals(strComps[i])) {
          comps[i] = "-1";
        } else if("beta".equals(strComps[i])) {
          comps[i] = "-2";
        } else if("alpha".equals(strComps[i])) {
          comps[i] = "-3";
        } else {
          comps[i] = "-4";
        }
      }
    }
    return comps;
  }

  public static int getMajorVersion(String version) {
    return Integer.parseInt(version.split("\\.")[0]);
  }

  public static void main(String[] args) {
    writeTo(System.out);
  }
}
