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
import org.apache.hudi.common.util.ValidationUtils;

/**
 * Exposes the <a href="https://semver.org/">semantic versioning</a> of HUDI code.
 *
 * HUDI versions are configured through maven and are formatted as <major>.<minor>.<patch>. Example: 0.12.2 or 0.12.3-snapshot
 */
public final class HoodieVersion {
  /**
   * Returns the complete version of HUDI code
   * Example: 0.12.2 or 0.12.3-snapshot
   */
  public static String get() {
    // This reads the property "Implementation-Version" of the generated META-INF/MANIFEST.MF (that is set to the pom.xml's version) in the archive
    // from where this class is loaded. Due to this, this is available only in release jars and not for unit-tests.
    String versionStr = HoodieVersion.class.getPackage().getImplementationVersion();
    if (StringUtils.isNullOrEmpty(versionStr)) {
      // Possible in unit tests
      versionStr = "0.0.0";
    }
    return versionStr;
  }

  /**
   * Returns the major version of HUDI code
   * Example: 0 for 0.12.2
   */
  public static String major() {
    return getVersionParts()[0];
  }

  /**
   * Returns the minor version of HUDI code
   * Example: 12 for 0.12.2
   */
  public static String minor() {
    return getVersionParts()[1];
  }

  /**
   * Returns the patch version of HUDI code
   * Example: 2 for 0.12.2, 3-SNAPSHOT for 0.12.3-SNAPSHOT
   */
  public static String patch() {
    return getVersionParts()[2];
  }

  private static String[] getVersionParts() {
    String[] parts = get().split("\\.");
    ValidationUtils.checkArgument(parts.length == 3, "HUDI version is not in the format major.minor.patch: " + get());
    return parts;
  }

  public static void main(String[] args) {
    System.out.println("hoodie version=" + get());
    System.out.println(" major=" + major());
    System.out.println(" minor=" + minor());
    System.out.println(" patch=" + patch());
  }
}