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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is a wrapper for the implementation of
 * com.sun.management.UnixOperatingSystemMXBean
 * It will decide to use the sun api or its own implementation
 * depending on the runtime (vendor) used.
 */

@InterfaceAudience.Private
public class JVM {
  private static final Logger LOG = LoggerFactory.getLogger(JVM.class);
  private OperatingSystemMXBean osMbean;

  private static final boolean ibmvendor =
      System.getProperty("java.vendor") != null &&
          System.getProperty("java.vendor").contains("IBM");
  private static final boolean windows =
      System.getProperty("os.name") != null &&
          System.getProperty("os.name").startsWith("Windows");
  private static final boolean linux =
      System.getProperty("os.name") != null &&
          System.getProperty("os.name").startsWith("Linux");
  private static final boolean amd64 =
      System.getProperty("os.arch") != null &&
          System.getProperty("os.arch").contains("amd64");

  private static final String JVMVersion = System.getProperty("java.version");

  /**
   * The raw String of java specification version.
   * "1.8" for java8, "9","10"... for Java 9, 10...
   */
  private static final String JVM_SPEC_VERSION_STRING =
      System.getProperty("java.specification.version");

  /**
   * The Integer represent of JVM_SPEC_VERSION, for the JVM version comparison.
   * Java 8, 9, 10 ... will be noted as 8, 9 10 ...
   */
  private static final int JVM_SPEC_VERSION = JVM_SPEC_VERSION_STRING.contains(".") ?
      (int) (Float.parseFloat(JVM_SPEC_VERSION_STRING) * 10 % 10) :
      Integer.parseInt(JVM_SPEC_VERSION_STRING);

  /**
   * Constructor. Get the running Operating System instance
   */
  public JVM() {
    this.osMbean = ManagementFactory.getOperatingSystemMXBean();
  }

  /**
   * Check if the OS is unix.
   *
   * @return whether this is unix or not.
   */
  public static boolean isUnix() {
    if (windows) {
      return false;
    }
    return (ibmvendor ? linux : true);
  }

  /**
   * Check if the OS is linux.
   *
   * @return whether this is linux or not.
   */
  public static boolean isLinux() {
    return linux;
  }

  /**
   * Check if the arch is amd64;
   *
   * @return whether this is amd64 or not.
   */
  public static boolean isAmd64() {
    return amd64;
  }

  /**
   * Check if the finish() method of GZIPOutputStream is broken
   *
   * @return whether GZIPOutputStream.finish() is broken.
   */
  public static boolean isGZIPOutputStreamFinishBroken() {
    return ibmvendor && JVMVersion.contains("1.6.0");
  }

  public static int getJVMSpecVersion() {
    return JVM_SPEC_VERSION;
  }

  /**
   * Load the implementation of UnixOperatingSystemMXBean for Oracle jvm
   * and runs the desired method.
   *
   * @param mBeanMethodName : method to run from the interface UnixOperatingSystemMXBean
   *
   * @return the method result
   */
  private Long runUnixMXBeanMethod(String mBeanMethodName) {
    Object unixos;
    Class<?> classRef;
    Method mBeanMethod;

    try {
      classRef = Class.forName("com.sun.management.UnixOperatingSystemMXBean");
      if (classRef.isInstance(osMbean)) {
        mBeanMethod = classRef.getMethod(mBeanMethodName);
        unixos = classRef.cast(osMbean);
        return (Long) mBeanMethod.invoke(unixos);
      }
    } catch (Exception e) {
      LOG.warn("Not able to load class or method for" +
          " com.sun.management.UnixOperatingSystemMXBean.", e);
    }
    return null;
  }

  /**
   * Get the number of opened filed descriptor for the runtime jvm.
   * If Oracle java, it will use the com.sun.management interfaces.
   * Otherwise, this methods implements it (linux only).
   *
   * @return number of open file descriptors for the jvm
   */
  public long getOpenFileDescriptorCount() {
    Long ofdc;

    if (!ibmvendor) {
      ofdc = runUnixMXBeanMethod("getOpenFileDescriptorCount");
      return (ofdc != null ? ofdc : -1);
    }
    InputStream inputStream = null;
    InputStreamReader inputStreamReader = null;
    BufferedReader bufferedReader = null;
    try {
      //need to get the PID number of the process first
      RuntimeMXBean rtmbean = ManagementFactory.getRuntimeMXBean();
      String rtname = rtmbean.getName();
      String[] pidhost = rtname.split("@");

      //using linux bash commands to retrieve info
      Process p = Runtime.getRuntime().exec(
          new String[]{"bash", "-c",
              "ls /proc/" + pidhost[0] + "/fdinfo | wc -l"});
      inputStream = p.getInputStream();
      inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
      bufferedReader = new BufferedReader(inputStreamReader);
      String openFileDesCount;
      if ((openFileDesCount = bufferedReader.readLine()) != null) {
        return Long.parseLong(openFileDesCount);
      }
    } catch (IOException ie) {
      LOG.warn("Not able to get the number of open file descriptors", ie);
    } finally {
      if (bufferedReader != null) {
        try {
          bufferedReader.close();
        } catch (IOException e) {
          LOG.warn("Not able to close the BufferedReader", e);
        }
      }
      if (inputStreamReader != null) {
        try {
          inputStreamReader.close();
        } catch (IOException e) {
          LOG.warn("Not able to close the InputStreamReader", e);
        }
      }
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException e) {
          LOG.warn("Not able to close the InputStream", e);
        }
      }
    }
    return -1;
  }

  /**
   * @see java.lang.management.OperatingSystemMXBean#getSystemLoadAverage
   */
  public double getSystemLoadAverage() {
    return osMbean.getSystemLoadAverage();
  }

  /**
   * @return the physical free memory (not the JVM one, as it's not very useful as it depends on
   * the GC), but the one from the OS as it allows a little bit more to guess if the machine is
   * overloaded or not).
   */
  public long getFreeMemory() {
    if (ibmvendor) {
      return 0;
    }

    Long r = runUnixMXBeanMethod("getFreePhysicalMemorySize");
    return (r != null ? r : -1);
  }


  /**
   * Workaround to get the current number of process running. Approach is the one described here:
   * http://stackoverflow.com/questions/54686/how-to-get-a-list-of-current-open-windows-process-with-java
   */
  public int getNumberOfRunningProcess() {
    if (!isUnix()) {
      return 0;
    }

    InputStream inputStream = null;
    InputStreamReader inputStreamReader = null;
    BufferedReader bufferedReader = null;

    try {
      int count = 0;
      Process p = Runtime.getRuntime().exec("ps -e");
      inputStream = p.getInputStream();
      inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
      bufferedReader = new BufferedReader(inputStreamReader);
      while (bufferedReader.readLine() != null) {
        count++;
      }
      return count - 1; //  -1 because there is a headline
    } catch (IOException e) {
      return -1;
    } finally {
      if (bufferedReader != null) {
        try {
          bufferedReader.close();
        } catch (IOException e) {
          LOG.warn("Not able to close the BufferedReader", e);
        }
      }
      if (inputStreamReader != null) {
        try {
          inputStreamReader.close();
        } catch (IOException e) {
          LOG.warn("Not able to close the InputStreamReader", e);
        }
      }
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException e) {
          LOG.warn("Not able to close the InputStream", e);
        }
      }
    }
  }

  /**
   * Get the number of the maximum file descriptors the system can use.
   * If Oracle java, it will use the com.sun.management interfaces.
   * Otherwise, this methods implements it (linux only).
   *
   * @return max number of file descriptors the operating system can use.
   */
  public long getMaxFileDescriptorCount() {
    Long mfdc;
    if (!ibmvendor) {
      mfdc = runUnixMXBeanMethod("getMaxFileDescriptorCount");
      return (mfdc != null ? mfdc : -1);
    }
    InputStream in = null;
    BufferedReader output = null;
    try {
      //using linux bash commands to retrieve info
      Process p = Runtime.getRuntime().exec(new String[]{"bash", "-c", "ulimit -n"});
      in = p.getInputStream();
      output = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
      String maxFileDesCount;
      if ((maxFileDesCount = output.readLine()) != null) {
        return Long.parseLong(maxFileDesCount);
      }
    } catch (IOException ie) {
      LOG.warn("Not able to get the max number of file descriptors", ie);
    } finally {
      if (output != null) {
        try {
          output.close();
        } catch (IOException e) {
          LOG.warn("Not able to close the reader", e);
        }
      }
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          LOG.warn("Not able to close the InputStream", e);
        }
      }
    }
    return -1;
  }
}
