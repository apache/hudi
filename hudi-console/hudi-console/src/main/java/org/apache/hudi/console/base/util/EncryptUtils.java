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

import org.apache.commons.codec.digest.DigestUtils;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

public class EncryptUtils {

  private static final int KEY_SIZE = 128;

  private static final String DEFAULT_KEY = DigestUtils.md5Hex("ApacheHudi");

  private static final String ALGORITHM = "AES";

  private static final String RNG_ALGORITHM = "SHA1PRNG";

  private EncryptUtils() {}

  public static String encrypt(String content) throws Exception {
    return encrypt(content, DEFAULT_KEY);
  }

  public static String encrypt(String content, String key) throws Exception {
    Cipher cipher = getCipher(Cipher.ENCRYPT_MODE, key);
    byte[] bytes = cipher.doFinal(content.getBytes(StandardCharsets.UTF_8));
    return Base64.getEncoder().encodeToString(bytes);
  }

  public static String decrypt(String content) throws Exception {
    return decrypt(content, DEFAULT_KEY);
  }

  public static String decrypt(String content, String key) throws Exception {
    Cipher cipher = getCipher(Cipher.DECRYPT_MODE, key);
    byte[] base64 = Base64.getDecoder().decode(content);
    byte[] decryptBytes = cipher.doFinal(base64);
    return new String(decryptBytes, StandardCharsets.UTF_8);
  }

  private static Cipher getCipher(int mode, String key) throws Exception {
    SecureRandom random = SecureRandom.getInstance(RNG_ALGORITHM);
    random.setSeed(key.getBytes(StandardCharsets.UTF_8));
    KeyGenerator gen = KeyGenerator.getInstance(ALGORITHM);
    gen.init(KEY_SIZE, random);
    SecretKey secKey = gen.generateKey();
    Cipher cipher = Cipher.getInstance(ALGORITHM);
    cipher.init(mode, secKey);
    return cipher;
  }
}
