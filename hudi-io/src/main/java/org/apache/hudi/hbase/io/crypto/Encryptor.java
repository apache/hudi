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

import java.io.OutputStream;
import java.security.Key;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Encryptors apply a cipher to an OutputStream to produce ciphertext.
 */
@InterfaceAudience.Public
public interface Encryptor {

  /**
   * Set the secret key
   * @param key
   */
  public void setKey(Key key);

  /**
   * Get the expected length for the initialization vector
   * @return the expected length for the initialization vector
   */
  public int getIvLength();

  /**
   * Get the cipher's internal block size
   * @return the cipher's internal block size
   */
  public int getBlockSize();

  /**
   * Get the initialization vector
   */
  public byte[] getIv();

  /**
   * Set the initialization vector
   * @param iv
   */
  public void setIv(byte[] iv);

  /**
   * Create a stream for encryption
   * @param out
   */
  public OutputStream createEncryptionStream(OutputStream out);

  /**
   * Reset state, reinitialize with the key and iv
   */
  void reset();
}
