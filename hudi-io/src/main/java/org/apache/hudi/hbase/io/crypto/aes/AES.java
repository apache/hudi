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

package org.apache.hudi.hbase.io.crypto.aes;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.SecureRandom;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hudi.hbase.io.crypto.Cipher;
import org.apache.hudi.hbase.io.crypto.CipherProvider;
import org.apache.hudi.hbase.io.crypto.Context;
import org.apache.hudi.hbase.io.crypto.Decryptor;
import org.apache.hudi.hbase.io.crypto.Encryptor;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * AES-128, provided by the JCE
 * <p>
 * Algorithm instances are pooled for reuse, so the cipher provider and mode
 * are configurable but fixed at instantiation.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AES extends Cipher {

  private static final Logger LOG = LoggerFactory.getLogger(AES.class);

  public static final String CIPHER_MODE_KEY = "hbase.crypto.algorithm.aes.mode";
  public static final String CIPHER_PROVIDER_KEY = "hbase.crypto.algorithm.aes.provider";

  private final String rngAlgorithm;
  private final String cipherMode;
  private final String cipherProvider;
  private SecureRandom rng;

  public AES(CipherProvider provider) {
    super(provider);
    // The JCE mode for Ciphers
    cipherMode = provider.getConf().get(CIPHER_MODE_KEY, "AES/CTR/NoPadding");
    // The JCE provider, null if default
    cipherProvider = provider.getConf().get(CIPHER_PROVIDER_KEY);
    // RNG algorithm
    rngAlgorithm = provider.getConf().get(RNG_ALGORITHM_KEY, "SHA1PRNG");
    // RNG provider, null if default
    String rngProvider = provider.getConf().get(RNG_PROVIDER_KEY);
    try {
      if (rngProvider != null) {
        rng = SecureRandom.getInstance(rngAlgorithm, rngProvider);
      } else {
        rng = SecureRandom.getInstance(rngAlgorithm);
      }
    } catch (GeneralSecurityException e) {
      LOG.warn("Could not instantiate specified RNG, falling back to default", e);
      rng = new SecureRandom();
    }
  }

  @Override
  public String getName() {
    return "AES";
  }

  @Override
  public int getKeyLength() {
    return KEY_LENGTH;
  }

  @Override
  public int getIvLength() {
    return IV_LENGTH;
  }

  @Override
  public Key getRandomKey() {
    byte[] keyBytes = new byte[getKeyLength()];
    rng.nextBytes(keyBytes);
    return new SecretKeySpec(keyBytes, getName());
  }

  @Override
  public Encryptor getEncryptor() {
    return new AESEncryptor(getJCECipherInstance(), rng);
  }

  @Override
  public Decryptor getDecryptor() {
    return new AESDecryptor(getJCECipherInstance());
  }

  @Override
  public OutputStream createEncryptionStream(OutputStream out, Context context, byte[] iv)
      throws IOException {
    Preconditions.checkNotNull(context);
    Preconditions.checkState(context.getKey() != null, "Context does not have a key");
    Preconditions.checkNotNull(iv);
    Encryptor e = getEncryptor();
    e.setKey(context.getKey());
    e.setIv(iv);
    return e.createEncryptionStream(out);
  }

  @Override
  public OutputStream createEncryptionStream(OutputStream out, Encryptor e) throws IOException {
    Preconditions.checkNotNull(e);
    return e.createEncryptionStream(out);
  }

  @Override
  public InputStream createDecryptionStream(InputStream in, Context context, byte[] iv)
      throws IOException {
    Preconditions.checkNotNull(context);
    Preconditions.checkState(context.getKey() != null, "Context does not have a key");
    Preconditions.checkNotNull(iv);
    Decryptor d = getDecryptor();
    d.setKey(context.getKey());
    d.setIv(iv);
    return d.createDecryptionStream(in);
  }

  @Override
  public InputStream createDecryptionStream(InputStream in, Decryptor d) throws IOException {
    Preconditions.checkNotNull(d);
    return d.createDecryptionStream(in);
  }

  SecureRandom getRNG() {
    return rng;
  }

  private javax.crypto.Cipher getJCECipherInstance() {
    try {
      if (cipherProvider != null) {
        return javax.crypto.Cipher.getInstance(cipherMode, cipherProvider);
      }
      return javax.crypto.Cipher.getInstance(cipherMode);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    }
  }

}
