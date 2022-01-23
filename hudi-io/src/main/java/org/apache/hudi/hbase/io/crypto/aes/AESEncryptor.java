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

import java.io.OutputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.SecureRandom;
import javax.crypto.spec.IvParameterSpec;

import org.apache.hudi.hbase.io.crypto.Encryptor;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AESEncryptor implements Encryptor {

  private javax.crypto.Cipher cipher;
  private SecureRandom rng;
  private Key key;
  private byte[] iv;
  private boolean initialized = false;

  public AESEncryptor(javax.crypto.Cipher cipher, SecureRandom rng) {
    this.cipher = cipher;
    this.rng = rng;
  }

  javax.crypto.Cipher getCipher() {
    return cipher;
  }

  @Override
  public void setKey(Key key) {
    this.key = key;
  }

  @Override
  public int getIvLength() {
    return AES.IV_LENGTH;
  }

  @Override
  public int getBlockSize() {
    return AES.BLOCK_SIZE;
  }

  @Override
  public byte[] getIv() {
    return iv;
  }

  @Override
  public void setIv(byte[] iv) {
    if (iv != null) {
      Preconditions.checkArgument(iv.length == AES.IV_LENGTH, "Invalid IV length");
    }
    this.iv = iv;
  }

  @Override
  public OutputStream createEncryptionStream(OutputStream out) {
    if (!initialized) {
      init();
    }
    return new javax.crypto.CipherOutputStream(out, cipher);
  }

  @Override
  public void reset() {
    init();
  }

  protected void init() {
    try {
      if (iv == null) {
        iv = new byte[getIvLength()];
        rng.nextBytes(iv);
      }
      cipher.init(javax.crypto.Cipher.ENCRYPT_MODE, key, new IvParameterSpec(iv));
    } catch (InvalidKeyException e) {
      throw new RuntimeException(e);
    } catch (InvalidAlgorithmParameterException e) {
      throw new RuntimeException(e);
    }
    initialized = true;
  }

}
