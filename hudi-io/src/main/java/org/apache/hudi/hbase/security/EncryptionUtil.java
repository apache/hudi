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

package org.apache.hudi.hbase.security;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.Key;
import java.security.KeyException;
import java.security.SecureRandom;
import java.util.Properties;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.hbase.HConstants;
import org.apache.hudi.hbase.client.ColumnFamilyDescriptor;
import org.apache.hudi.hbase.io.crypto.Cipher;
import org.apache.hudi.hbase.io.crypto.Encryption;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hudi.hbase.shaded.protobuf.generated.EncryptionProtos;
import org.apache.hudi.hbase.shaded.protobuf.generated.RPCProtos;

/**
 * Some static utility methods for encryption uses in hbase-client.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class EncryptionUtil {
  static private final Logger LOG = LoggerFactory.getLogger(EncryptionUtil.class);

  static private final SecureRandom RNG = new SecureRandom();

  /**
   * Private constructor to keep this class from being instantiated.
   */
  private EncryptionUtil() {
  }

  /**
   * Protect a key by encrypting it with the secret key of the given subject.
   * The configuration must be set up correctly for key alias resolution.
   * @param conf configuration
   * @param subject subject key alias
   * @param key the key
   * @return the encrypted key bytes
   */
  public static byte[] wrapKey(Configuration conf, String subject, Key key)
      throws IOException {
    // Wrap the key with the configured encryption algorithm.
    String algorithm =
        conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    Cipher cipher = Encryption.getCipher(conf, algorithm);
    if (cipher == null) {
      throw new RuntimeException("Cipher '" + algorithm + "' not available");
    }
    EncryptionProtos.WrappedKey.Builder builder = EncryptionProtos.WrappedKey.newBuilder();
    builder.setAlgorithm(key.getAlgorithm());
    byte[] iv = null;
    if (cipher.getIvLength() > 0) {
      iv = new byte[cipher.getIvLength()];
      RNG.nextBytes(iv);
      builder.setIv(UnsafeByteOperations.unsafeWrap(iv));
    }
    byte[] keyBytes = key.getEncoded();
    builder.setLength(keyBytes.length);
    builder.setHashAlgorithm(Encryption.getConfiguredHashAlgorithm(conf));
    builder.setHash(
        UnsafeByteOperations.unsafeWrap(Encryption.computeCryptoKeyHash(conf, keyBytes)));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encryption.encryptWithSubjectKey(out, new ByteArrayInputStream(keyBytes), subject,
        conf, cipher, iv);
    builder.setData(UnsafeByteOperations.unsafeWrap(out.toByteArray()));
    // Build and return the protobuf message
    out.reset();
    builder.build().writeDelimitedTo(out);
    return out.toByteArray();
  }

  /**
   * Unwrap a key by decrypting it with the secret key of the given subject.
   * The configuration must be set up correctly for key alias resolution.
   * @param conf configuration
   * @param subject subject key alias
   * @param value the encrypted key bytes
   * @return the raw key bytes
   * @throws IOException
   * @throws KeyException
   */
  public static Key unwrapKey(Configuration conf, String subject, byte[] value)
      throws IOException, KeyException {
    EncryptionProtos.WrappedKey wrappedKey = EncryptionProtos.WrappedKey.PARSER
        .parseDelimitedFrom(new ByteArrayInputStream(value));
    String algorithm = conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY,
        HConstants.CIPHER_AES);
    Cipher cipher = Encryption.getCipher(conf, algorithm);
    if (cipher == null) {
      throw new RuntimeException("Cipher '" + algorithm + "' not available");
    }
    return getUnwrapKey(conf, subject, wrappedKey, cipher);
  }

  private static Key getUnwrapKey(Configuration conf, String subject,
                                  EncryptionProtos.WrappedKey wrappedKey, Cipher cipher) throws IOException, KeyException {
    String configuredHashAlgorithm = Encryption.getConfiguredHashAlgorithm(conf);
    String wrappedHashAlgorithm = wrappedKey.getHashAlgorithm().trim();
    if(!configuredHashAlgorithm.equalsIgnoreCase(wrappedHashAlgorithm)) {
      String msg = String.format("Unexpected encryption key hash algorithm: %s (expecting: %s)",
          wrappedHashAlgorithm, configuredHashAlgorithm);
      if(Encryption.failOnHashAlgorithmMismatch(conf)) {
        throw new KeyException(msg);
      }
      LOG.debug(msg);
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] iv = wrappedKey.hasIv() ? wrappedKey.getIv().toByteArray() : null;
    Encryption.decryptWithSubjectKey(out, wrappedKey.getData().newInput(),
        wrappedKey.getLength(), subject, conf, cipher, iv);
    byte[] keyBytes = out.toByteArray();
    if (wrappedKey.hasHash()) {
      if (!Bytes.equals(wrappedKey.getHash().toByteArray(),
          Encryption.hashWithAlg(wrappedHashAlgorithm, keyBytes))) {
        throw new KeyException("Key was not successfully unwrapped");
      }
    }
    return new SecretKeySpec(keyBytes, wrappedKey.getAlgorithm());
  }

  /**
   * Helper to create an encyption context.
   *
   * @param conf The current configuration.
   * @param family The current column descriptor.
   * @return The created encryption context.
   * @throws IOException if an encryption key for the column cannot be unwrapped
   * @throws IllegalStateException in case of encryption related configuration errors
   */
  public static Encryption.Context createEncryptionContext(Configuration conf,
                                                           ColumnFamilyDescriptor family) throws IOException {
    Encryption.Context cryptoContext = Encryption.Context.NONE;
    String cipherName = family.getEncryptionType();
    if (cipherName != null) {
      if(!Encryption.isEncryptionEnabled(conf)) {
        throw new IllegalStateException("Encryption for family '" + family.getNameAsString()
            + "' configured with type '" + cipherName + "' but the encryption feature is disabled");
      }
      Cipher cipher;
      Key key;
      byte[] keyBytes = family.getEncryptionKey();
      if (keyBytes != null) {
        // Family provides specific key material
        key = unwrapKey(conf, keyBytes);
        // Use the algorithm the key wants
        cipher = Encryption.getCipher(conf, key.getAlgorithm());
        if (cipher == null) {
          throw new IllegalStateException("Cipher '" + key.getAlgorithm() + "' is not available");
        }
        // Fail if misconfigured
        // We use the encryption type specified in the column schema as a sanity check on
        // what the wrapped key is telling us
        if (!cipher.getName().equalsIgnoreCase(cipherName)) {
          throw new IllegalStateException("Encryption for family '" + family.getNameAsString()
              + "' configured with type '" + cipherName + "' but key specifies algorithm '"
              + cipher.getName() + "'");
        }
      } else {
        // Family does not provide key material, create a random key
        cipher = Encryption.getCipher(conf, cipherName);
        if (cipher == null) {
          throw new IllegalStateException("Cipher '" + cipherName + "' is not available");
        }
        key = cipher.getRandomKey();
      }
      cryptoContext = Encryption.newContext(conf);
      cryptoContext.setCipher(cipher);
      cryptoContext.setKey(key);
    }
    return cryptoContext;
  }

  /**
   * Helper for {@link #unwrapKey(Configuration, String, byte[])} which automatically uses the
   * configured master and alternative keys, rather than having to specify a key type to unwrap
   * with.
   *
   * The configuration must be set up correctly for key alias resolution.
   *
   * @param conf the current configuration
   * @param keyBytes the key encrypted by master (or alternative) to unwrap
   * @return the key bytes, decrypted
   * @throws IOException if the key cannot be unwrapped
   */
  public static Key unwrapKey(Configuration conf, byte[] keyBytes) throws IOException {
    Key key;
    String masterKeyName = conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY,
        User.getCurrent().getShortName());
    try {
      // First try the master key
      key = unwrapKey(conf, masterKeyName, keyBytes);
    } catch (KeyException e) {
      // If the current master key fails to unwrap, try the alternate, if
      // one is configured
      if (LOG.isDebugEnabled()) {
        LOG.debug("Unable to unwrap key with current master key '" + masterKeyName + "'");
      }
      String alternateKeyName =
          conf.get(HConstants.CRYPTO_MASTERKEY_ALTERNATE_NAME_CONF_KEY);
      if (alternateKeyName != null) {
        try {
          key = unwrapKey(conf, alternateKeyName, keyBytes);
        } catch (KeyException ex) {
          throw new IOException(ex);
        }
      } else {
        throw new IOException(e);
      }
    }
    return key;
  }
}
