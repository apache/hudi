/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.console.base.util;

import org.apache.shiro.crypto.hash.Sha256Hash;
import org.apache.shiro.util.ByteSource;

import java.util.Random;

public final class ShaHashUtils {

  public static final int DEFAULT_SALT_LENGTH = 26;

  private ShaHashUtils() {}

  /**
   * encrypt user password
   *
   * @param salt
   * @param password
   * @return
   */
  public static String encrypt(String salt, String password) {
    return new Sha256Hash(password, ByteSource.Util.bytes(salt), 1024).toHex();
  }

  public static String getRandomSalt() {
    return getRandomSalt(DEFAULT_SALT_LENGTH);
  }

  /**
   * get random salt
   *
   * @param length
   * @return
   */
  public static String getRandomSalt(int length) {
    String base = "abcdefghijklmnopqrstuvwxyz0123456789";
    Random random = new Random();
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < length; i++) {
      int number = random.nextInt(base.length());
      builder.append(base.charAt(number));
    }
    return builder.toString();
  }
}
