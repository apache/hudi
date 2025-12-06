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

package org.apache.hudi;

import org.apache.hudi.common.util.StringUtils;

import java.io.InputStream;
import java.util.Properties;

/**
 * Exposes the semantic versioning (https://semver.org/) of HUDI code.
 *
 * HUDI versions are configured through maven and are formatted as <major>.<minor>.<patch>. Example: 0.12.2 or 0.12.3-snapshot
 */
public final class HoodieVersion {
  private static String HOODIE_DEFAULT_VERSION = "0.14.x";

  public static final String HOODIE_WRITER_VERSION = "hudi_writer_version";

  /**
   * Returns the complete version of HUDI code
   * Example: 0.12.2 or 0.12.3-snapshot
   */
  public static String get() {
    String hudiPropertiesFilePath = "META-INF/maven/org.apache.hudi/hudi-common/pom.properties";
    try (InputStream inputStream = HoodieVersion.class.getClassLoader().getResourceAsStream(hudiPropertiesFilePath)) {
      Properties properties = new Properties();
      if (inputStream != null) {
        properties.load(inputStream);
        // Access properties
        return properties.getProperty("version");
      }
    } catch (Exception ignored) {
      // Ignoring the exception as there is as fallback to default version
    }
    return HOODIE_DEFAULT_VERSION;
  }

  /**
   * Returns the major version of HUDI code
   * Example: 0 for 0.12.2
   */
  public static String major() {
    return getVersion(1);
  }

  /**
   * Returns the minor version of HUDI code
   * Example: 0.12 for 0.12.2
   */
  public static String minor() {
    return getVersion(2);
  }

  /**
   * Returns the patch version of HUDI code
   * Example: 0.12.2 for 0.12.2, 0.12.3-SNAPSHOT for 0.12.3-SNAPSHOT
   */
  public static String patch() {
    return getVersion(3);
  }

  private static String getVersion(int allowedDotCount) {
    return getSubVersion(get(), allowedDotCount);
  }

  private static String getSubVersion(String version, int allowedDotCount) {
    if (allowedDotCount == 1) {
      int index = version.indexOf('.');
      return index == -1 ? version : version.substring(0, index);
    }
    return getSubVersion(version.substring(version.indexOf('.') + 1), allowedDotCount - 1);
  }

  protected static void setVersionOverride(String version) {
    HOODIE_DEFAULT_VERSION = version;
  }

  public static void main(String[] args) {
    System.out.println("hoodie version=" + get());
    System.out.println(" major=" + major());
    System.out.println(" minor=" + minor());
    System.out.println(" patch=" + patch());
  }

  /**
   * Returns the major version of HUDI code as an integer
   */
  public static int majorAsInt() {
    return toInt(major());
  }

  /**
   * Returns the minor version of HUDI code as an integer
   */
  public static int minorAsInt() {
    return toInt(minor());
  }

  /**
   * Returns the patch version of HUDI code as an integer
   */
  public static int patchAsInt() {
    return toInt(patch());
  }

  /**
   * Convert the string to an integer ignoring any non-numeric characters towards the end of the string.
   * @param str The string to convert.
   * @return The integer value of the string. Returns 0 if the string is null, empty or does not have any numeric values.
   */
  private static int toInt(String str) {
    if (StringUtils.isNullOrEmpty(str)) {
      return 0;
    }

    // Find the number of digits in the starting of the string.
    int i = 0;
    while (i < str.length() && Character.isDigit(str.charAt(i))) {
      i++;
    }
    if (i > 0) {
      return Integer.parseInt(str.substring(0, i));
    }
    return 0;
  }
}
