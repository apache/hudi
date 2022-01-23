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

package org.apache.hudi.hbase.io.crypto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.Key;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A common interface for a cryptographic algorithm.
 */
@InterfaceAudience.Public
public abstract class Cipher {

  public static final int KEY_LENGTH = 16;
  public static final int KEY_LENGTH_BITS = KEY_LENGTH * 8;
  public static final int BLOCK_SIZE = 16;
  public static final int IV_LENGTH = 16;

  public static final String RNG_ALGORITHM_KEY = "hbase.crypto.algorithm.rng";
  public static final String RNG_PROVIDER_KEY = "hbase.crypto.algorithm.rng.provider";

  private final CipherProvider provider;

  public Cipher(CipherProvider provider) {
    this.provider = provider;
  }

  /**
   * Return the provider for this Cipher
   */
  public CipherProvider getProvider() {
    return provider;
  }

  /**
   * Return this Cipher's name
   */
  public abstract String getName();

  /**
   * Return the key length required by this cipher, in bytes
   */
  public abstract int getKeyLength();

  /**
   * Return the expected initialization vector length, in bytes, or 0 if not applicable
   */
  public abstract int getIvLength();

  /**
   * Create a random symmetric key
   * @return the random symmetric key
   */
  public abstract Key getRandomKey();

  /**
   * Get an encryptor for encrypting data.
   */
  public abstract Encryptor getEncryptor();

  /**
   * Return a decryptor for decrypting data.
   */
  public abstract Decryptor getDecryptor();

  /**
   * Create an encrypting output stream given a context and IV
   * @param out the output stream to wrap
   * @param context the encryption context
   * @param iv initialization vector
   * @return the encrypting wrapper
   * @throws IOException
   */
  public abstract OutputStream createEncryptionStream(OutputStream out, Context context,
                                                      byte[] iv)
      throws IOException;

  /**
   * Create an encrypting output stream given an initialized encryptor
   * @param out the output stream to wrap
   * @param encryptor the encryptor
   * @return the encrypting wrapper
   * @throws IOException
   */
  public abstract OutputStream createEncryptionStream(OutputStream out, Encryptor encryptor)
      throws IOException;

  /**
   * Create a decrypting input stream given a context and IV
   * @param in the input stream to wrap
   * @param context the encryption context
   * @param iv initialization vector
   * @return the decrypting wrapper
   * @throws IOException
   */
  public abstract InputStream createDecryptionStream(InputStream in, Context context,
                                                     byte[] iv)
      throws IOException;

  /**
   * Create a decrypting output stream given an initialized decryptor
   * @param in the input stream to wrap
   * @param decryptor the decryptor
   * @return the decrypting wrapper
   * @throws IOException
   */
  public abstract InputStream createDecryptionStream(InputStream in, Decryptor decryptor)
      throws IOException;

}
