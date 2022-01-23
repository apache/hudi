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

import static java.lang.String.format;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.hbase.HBaseConfiguration;
import org.apache.hudi.hbase.HConstants;
import org.apache.hudi.hbase.io.crypto.aes.AES;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.hudi.hbase.util.Pair;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A facade for encryption algorithms and related support.
 */
@InterfaceAudience.Public
public final class Encryption {

  private static final Logger LOG = LoggerFactory.getLogger(Encryption.class);


  /**
   * Configuration key for globally enable / disable column family encryption
   */
  public static final String CRYPTO_ENABLED_CONF_KEY = "hbase.crypto.enabled";

  /**
   * Default value for globally enable / disable column family encryption
   * (set to "true" for backward compatibility)
   */
  public static final boolean CRYPTO_ENABLED_CONF_DEFAULT = true;

  /**
   * Configuration key for the hash algorithm used for generating key hash in encrypted HFiles.
   * This is a MessageDigest algorithm identifier string, like "MD5", "SHA-256" or "SHA-384".
   * (default: "MD5" for backward compatibility reasons)
   */
  public static final String CRYPTO_KEY_HASH_ALGORITHM_CONF_KEY = "hbase.crypto.key.hash.algorithm";

  /**
   * Default hash algorithm used for generating key hash in encrypted HFiles.
   * (we use "MD5" for backward compatibility reasons)
   */
  public static final String CRYPTO_KEY_HASH_ALGORITHM_CONF_DEFAULT = "MD5";

  /**
   * Configuration key for specifying the behaviour if the configured hash algorithm
   * differs from the one used for generating key hash in encrypted HFiles currently being read.
   *
   * - "false" (default): we won't fail but use the hash algorithm stored in the HFile
   * - "true": we throw an exception (this can be useful if regulations are enforcing the usage
   *           of certain algorithms, e.g. on FIPS compliant clusters)
   */
  public static final String CRYPTO_KEY_FAIL_ON_ALGORITHM_MISMATCH_CONF_KEY =
      "hbase.crypto.key.hash.algorithm.failOnMismatch";

  /**
   * Default behaviour is not to fail if the hash algorithm configured differs from the one
   * used in the HFile. (this is the more fail-safe approach, allowing us to read
   * encrypted HFiles written using a different encryption key hash algorithm)
   */
  public static final boolean CRYPTO_KEY_FAIL_ON_ALGORITHM_MISMATCH_CONF_DEFAULT = false;


  /**
   * Crypto context
   */
  @InterfaceAudience.Public
  public static class Context extends org.apache.hudi.hbase.io.crypto.Context {

    /** The null crypto context */
    public static final Context NONE = new Context();

    private Context() {
      super();
    }

    private Context(Configuration conf) {
      super(conf);
    }

    @Override
    public Context setCipher(Cipher cipher) {
      super.setCipher(cipher);
      return this;
    }

    @Override
    public Context setKey(Key key) {
      super.setKey(key);
      return this;
    }

    public Context setKey(byte[] key) {
      super.setKey(new SecretKeySpec(key, getCipher().getName()));
      return this;
    }
  }

  public static Context newContext() {
    return new Context();
  }

  public static Context newContext(Configuration conf) {
    return new Context(conf);
  }

  // Prevent instantiation
  private Encryption() {
    super();
  }


  /**
   * Returns true if the column family encryption feature is enabled globally.
   */
  public static boolean isEncryptionEnabled(Configuration conf) {
    return conf.getBoolean(CRYPTO_ENABLED_CONF_KEY, CRYPTO_ENABLED_CONF_DEFAULT);
  }

  /**
   * Get an cipher given a name
   * @param name the cipher name
   * @return the cipher, or null if a suitable one could not be found
   */
  public static Cipher getCipher(Configuration conf, String name) {
    return getCipherProvider(conf).getCipher(name);
  }

  /**
   * Get names of supported encryption algorithms
   *
   * @return Array of strings, each represents a supported encryption algorithm
   */
  public static String[] getSupportedCiphers() {
    return getSupportedCiphers(HBaseConfiguration.create());
  }

  /**
   * Get names of supported encryption algorithms
   *
   * @return Array of strings, each represents a supported encryption algorithm
   */
  public static String[] getSupportedCiphers(Configuration conf) {
    return getCipherProvider(conf).getSupportedCiphers();
  }

  /**
   * Returns the Hash Algorithm defined in the crypto configuration.
   */
  public static String getConfiguredHashAlgorithm(Configuration conf) {
    return conf.getTrimmed(CRYPTO_KEY_HASH_ALGORITHM_CONF_KEY,
        CRYPTO_KEY_HASH_ALGORITHM_CONF_DEFAULT);
  }

  /**
   * Returns the Hash Algorithm mismatch behaviour defined in the crypto configuration.
   */
  public static boolean failOnHashAlgorithmMismatch(Configuration conf) {
    return conf.getBoolean(CRYPTO_KEY_FAIL_ON_ALGORITHM_MISMATCH_CONF_KEY,
        CRYPTO_KEY_FAIL_ON_ALGORITHM_MISMATCH_CONF_DEFAULT);
  }

  /**
   * Returns the hash of the supplied argument, using the hash algorithm
   * specified in the given config.
   */
  public static byte[] computeCryptoKeyHash(Configuration conf, byte[] arg) {
    String algorithm = getConfiguredHashAlgorithm(conf);
    try {
      return hashWithAlg(algorithm, arg);
    } catch (RuntimeException e) {
      String message = format("Error in computeCryptoKeyHash (please check your configuration " +
              "parameter %s and the security provider configuration of the JVM)",
          CRYPTO_KEY_HASH_ALGORITHM_CONF_KEY);
      throw new RuntimeException(message, e);
    }
  }

  /**
   * Return the MD5 digest of the concatenation of the supplied arguments.
   */
  public static byte[] hash128(String... args) {
    return hashWithAlg("MD5", Bytes.toByteArrays(args));
  }

  /**
   * Return the MD5 digest of the concatenation of the supplied arguments.
   */
  public static byte[] hash128(byte[]... args) {
    return hashWithAlg("MD5", args);
  }

  /**
   * Return the SHA-256 digest of the concatenation of the supplied arguments.
   */
  public static byte[] hash256(String... args) {
    return hashWithAlg("SHA-256", Bytes.toByteArrays(args));
  }

  /**
   * Return the SHA-256 digest of the concatenation of the supplied arguments.
   */
  public static byte[] hash256(byte[]... args) {
    return hashWithAlg("SHA-256", args);
  }

  /**
   * Return a 128 bit key derived from the concatenation of the supplied
   * arguments using PBKDF2WithHmacSHA1 at 10,000 iterations.
   *
   */
  public static byte[] pbkdf128(String... args) {
    StringBuilder sb = new StringBuilder();
    for (String s: args) {
      sb.append(s);
    }
    return generateSecretKey("PBKDF2WithHmacSHA1", AES.KEY_LENGTH, sb.toString().toCharArray());
  }

  /**
   * Return a 128 bit key derived from the concatenation of the supplied
   * arguments using PBKDF2WithHmacSHA1 at 10,000 iterations.
   *
   */
  public static byte[] pbkdf128(byte[]... args) {
    StringBuilder sb = new StringBuilder();
    for (byte[] b: args) {
      sb.append(Arrays.toString(b));
    }
    return generateSecretKey("PBKDF2WithHmacSHA1", AES.KEY_LENGTH, sb.toString().toCharArray());
  }

  /**
   * Return a key derived from the concatenation of the supplied arguments using
   * PBKDF2WithHmacSHA384 key derivation algorithm at 10,000 iterations.
   *
   * The length of the returned key is determined based on the need of the cypher algorithm.
   * E.g. for the default "AES"  we will need a 128 bit long key, while if the user is using
   * a custom cipher, we might generate keys with other length.
   *
   * This key generation method is used currently e.g. in the HBase Shell (admin.rb) to generate a
   * column family data encryption key, if the user provided an ENCRYPTION_KEY parameter.
   */
  public static byte[] generateSecretKey(Configuration conf, String cypherAlg, String... args) {
    StringBuilder sb = new StringBuilder();
    for (String s: args) {
      sb.append(s);
    }
    int keyLengthBytes = Encryption.getCipher(conf, cypherAlg).getKeyLength();
    return generateSecretKey("PBKDF2WithHmacSHA384", keyLengthBytes, sb.toString().toCharArray());
  }

  /**
   * Return a key derived from the concatenation of the supplied arguments using
   * PBKDF2WithHmacSHA384 key derivation algorithm at 10,000 iterations.
   *
   * The length of the returned key is determined based on the need of the cypher algorithm.
   * E.g. for the default "AES"  we will need a 128 bit long key, while if the user is using
   * a custom cipher, we might generate keys with other length.
   *
   * This key generation method is used currently e.g. in the HBase Shell (admin.rb) to generate a
   * column family data encryption key, if the user provided an ENCRYPTION_KEY parameter.
   */
  public static byte[] generateSecretKey(Configuration conf, String cypherAlg, byte[]... args) {
    StringBuilder sb = new StringBuilder();
    for (byte[] b: args) {
      sb.append(Arrays.toString(b));
    }
    int keyLength = Encryption.getCipher(conf, cypherAlg).getKeyLength();
    return generateSecretKey("PBKDF2WithHmacSHA384", keyLength, sb.toString().toCharArray());
  }

  /**
   * Return a key (byte array) derived from the supplied password argument using the given
   * algorithm with a random salt at 10,000 iterations.
   *
   * @param algorithm the secret key generation algorithm to use
   * @param keyLengthBytes the length of the key to be derived (in bytes, not in bits)
   * @param password char array to use as password for the key generation algorithm
   * @return secret key encoded as a byte array
   */
  private static byte[] generateSecretKey(String algorithm, int keyLengthBytes, char[] password) {
    byte[] salt = new byte[keyLengthBytes];
    Bytes.random(salt);
    PBEKeySpec spec = new PBEKeySpec(password, salt, 10000, keyLengthBytes*8);
    try {
      return SecretKeyFactory.getInstance(algorithm).generateSecret(spec).getEncoded();
    } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Encrypt a block of plaintext
   * <p>
   * The encryptor's state will be finalized. It should be reinitialized or
   * returned to the pool.
   * @param out ciphertext
   * @param src plaintext
   * @param offset
   * @param length
   * @param e
   * @throws IOException
   */
  public static void encrypt(OutputStream out, byte[] src, int offset,
                             int length, Encryptor e) throws IOException {
    OutputStream cout = e.createEncryptionStream(out);
    try {
      cout.write(src, offset, length);
    } finally {
      cout.close();
    }
  }

  /**
   * Encrypt a block of plaintext
   * @param out ciphertext
   * @param src plaintext
   * @param offset
   * @param length
   * @param context
   * @param iv
   * @throws IOException
   */
  public static void encrypt(OutputStream out, byte[] src, int offset,
                             int length, Context context, byte[] iv) throws IOException {
    Encryptor e = context.getCipher().getEncryptor();
    e.setKey(context.getKey());
    e.setIv(iv); // can be null
    e.reset();
    encrypt(out, src, offset, length, e);
  }

  /**
   * Encrypt a stream of plaintext given an encryptor
   * <p>
   * The encryptor's state will be finalized. It should be reinitialized or
   * returned to the pool.
   * @param out ciphertext
   * @param in plaintext
   * @param e
   * @throws IOException
   */
  public static void encrypt(OutputStream out, InputStream in, Encryptor e)
      throws IOException {
    OutputStream cout = e.createEncryptionStream(out);
    try {
      IOUtils.copy(in, cout);
    } finally {
      cout.close();
    }
  }

  /**
   * Encrypt a stream of plaintext given a context and IV
   * @param out ciphertext
   * @param in plaintet
   * @param context
   * @param iv
   * @throws IOException
   */
  public static void encrypt(OutputStream out, InputStream in, Context context,
                             byte[] iv) throws IOException {
    Encryptor e = context.getCipher().getEncryptor();
    e.setKey(context.getKey());
    e.setIv(iv); // can be null
    e.reset();
    encrypt(out, in, e);
  }

  /**
   * Decrypt a block of ciphertext read in from a stream with the given
   * cipher and context
   * <p>
   * The decryptor's state will be finalized. It should be reinitialized or
   * returned to the pool.
   * @param dest
   * @param destOffset
   * @param in
   * @param destSize
   * @param d
   * @throws IOException
   */
  public static void decrypt(byte[] dest, int destOffset, InputStream in,
                             int destSize, Decryptor d) throws IOException {
    InputStream cin = d.createDecryptionStream(in);
    try {
      IOUtils.readFully(cin, dest, destOffset, destSize);
    } finally {
      cin.close();
    }
  }

  /**
   * Decrypt a block of ciphertext from a stream given a context and IV
   * @param dest
   * @param destOffset
   * @param in
   * @param destSize
   * @param context
   * @param iv
   * @throws IOException
   */
  public static void decrypt(byte[] dest, int destOffset, InputStream in,
                             int destSize, Context context, byte[] iv) throws IOException {
    Decryptor d = context.getCipher().getDecryptor();
    d.setKey(context.getKey());
    d.setIv(iv); // can be null
    decrypt(dest, destOffset, in, destSize, d);
  }

  /**
   * Decrypt a stream of ciphertext given a decryptor
   * @param out
   * @param in
   * @param outLen
   * @param d
   * @throws IOException
   */
  public static void decrypt(OutputStream out, InputStream in, int outLen,
                             Decryptor d) throws IOException {
    InputStream cin = d.createDecryptionStream(in);
    byte buf[] = new byte[8*1024];
    long remaining = outLen;
    try {
      while (remaining > 0) {
        int toRead = (int)(remaining < buf.length ? remaining : buf.length);
        int read = cin.read(buf, 0, toRead);
        if (read < 0) {
          break;
        }
        out.write(buf, 0, read);
        remaining -= read;
      }
    } finally {
      cin.close();
    }
  }

  /**
   * Decrypt a stream of ciphertext given a context and IV
   * @param out
   * @param in
   * @param outLen
   * @param context
   * @param iv
   * @throws IOException
   */
  public static void decrypt(OutputStream out, InputStream in, int outLen,
                             Context context, byte[] iv) throws IOException {
    Decryptor d = context.getCipher().getDecryptor();
    d.setKey(context.getKey());
    d.setIv(iv); // can be null
    decrypt(out, in, outLen, d);
  }

  /**
   * Resolves a key for the given subject
   * @param subject
   * @param conf
   * @return a key for the given subject
   * @throws IOException if the key is not found
   */
  public static Key getSecretKeyForSubject(String subject, Configuration conf)
      throws IOException {
    KeyProvider provider = getKeyProvider(conf);
    if (provider != null) try {
      Key[] keys = provider.getKeys(new String[] { subject });
      if (keys != null && keys.length > 0) {
        return keys[0];
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
    throw new IOException("No key found for subject '" + subject + "'");
  }

  /**
   * Encrypts a block of plaintext with the symmetric key resolved for the given subject
   * @param out ciphertext
   * @param in plaintext
   * @param conf configuration
   * @param cipher the encryption algorithm
   * @param iv the initialization vector, can be null
   * @throws IOException
   */
  public static void encryptWithSubjectKey(OutputStream out, InputStream in,
                                           String subject, Configuration conf, Cipher cipher, byte[] iv)
      throws IOException {
    Key key = getSecretKeyForSubject(subject, conf);
    if (key == null) {
      throw new IOException("No key found for subject '" + subject + "'");
    }
    Encryptor e = cipher.getEncryptor();
    e.setKey(key);
    e.setIv(iv); // can be null
    encrypt(out, in, e);
  }

  /**
   * Decrypts a block of ciphertext with the symmetric key resolved for the given subject
   * @param out plaintext
   * @param in ciphertext
   * @param outLen the expected plaintext length
   * @param subject the subject's key alias
   * @param conf configuration
   * @param cipher the encryption algorithm
   * @param iv the initialization vector, can be null
   * @throws IOException
   */
  public static void decryptWithSubjectKey(OutputStream out, InputStream in, int outLen,
                                           String subject, Configuration conf, Cipher cipher, byte[] iv) throws IOException {
    Key key = getSecretKeyForSubject(subject, conf);
    if (key == null) {
      throw new IOException("No key found for subject '" + subject + "'");
    }
    Decryptor d = cipher.getDecryptor();
    d.setKey(key);
    d.setIv(iv); // can be null
    try {
      decrypt(out, in, outLen, d);
    } catch (IOException e) {
      // If the current cipher algorithm fails to unwrap, try the alternate cipher algorithm, if one
      // is configured
      String alternateAlgorithm = conf.get(HConstants.CRYPTO_ALTERNATE_KEY_ALGORITHM_CONF_KEY);
      if (alternateAlgorithm != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Unable to decrypt data with current cipher algorithm '"
              + conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES)
              + "'. Trying with the alternate cipher algorithm '" + alternateAlgorithm
              + "' configured.");
        }
        Cipher alterCipher = Encryption.getCipher(conf, alternateAlgorithm);
        if (alterCipher == null) {
          throw new RuntimeException("Cipher '" + alternateAlgorithm + "' not available");
        }
        d = alterCipher.getDecryptor();
        d.setKey(key);
        d.setIv(iv); // can be null
        decrypt(out, in, outLen, d);
      } else {
        throw new IOException(e);
      }
    }
  }

  private static ClassLoader getClassLoaderForClass(Class<?> c) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if (cl == null) {
      cl = c.getClassLoader();
    }
    if (cl == null) {
      cl = ClassLoader.getSystemClassLoader();
    }
    if (cl == null) {
      throw new RuntimeException("A ClassLoader to load the Cipher could not be determined");
    }
    return cl;
  }

  public static CipherProvider getCipherProvider(Configuration conf) {
    String providerClassName = conf.get(HConstants.CRYPTO_CIPHERPROVIDER_CONF_KEY,
        DefaultCipherProvider.class.getName());
    try {
      CipherProvider provider = (CipherProvider)
          ReflectionUtils.newInstance(getClassLoaderForClass(CipherProvider.class)
                  .loadClass(providerClassName),
              conf);
      return provider;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static final Map<Pair<String,String>,KeyProvider> keyProviderCache = new ConcurrentHashMap<>();

  public static KeyProvider getKeyProvider(Configuration conf) {
    String providerClassName = conf.get(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY,
        KeyStoreKeyProvider.class.getName());
    String providerParameters = conf.get(HConstants.CRYPTO_KEYPROVIDER_PARAMETERS_KEY, "");
    try {
      Pair<String,String> providerCacheKey = new Pair<>(providerClassName,
          providerParameters);
      KeyProvider provider = keyProviderCache.get(providerCacheKey);
      if (provider != null) {
        return provider;
      }
      provider = (KeyProvider) ReflectionUtils.newInstance(
          getClassLoaderForClass(KeyProvider.class).loadClass(providerClassName),
          conf);
      provider.init(providerParameters);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Installed " + providerClassName + " into key provider cache");
      }
      keyProviderCache.put(providerCacheKey, provider);
      return provider;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void incrementIv(byte[] iv) {
    incrementIv(iv, 1);
  }

  public static void incrementIv(byte[] iv, int v) {
    int length = iv.length;
    boolean carry = true;
    // TODO: Optimize for v > 1, e.g. 16, 32
    do {
      for (int i = 0; i < length; i++) {
        if (carry) {
          iv[i] = (byte) ((iv[i] + 1) & 0xFF);
          carry = 0 == iv[i];
        } else {
          break;
        }
      }
      v--;
    } while (v > 0);
  }

  /**
   * Return the hash of the concatenation of the supplied arguments, using the
   * hash algorithm provided.
   */
  public static byte[] hashWithAlg(String algorithm, byte[]... args) {
    try {
      MessageDigest md = MessageDigest.getInstance(algorithm);
      for (byte[] arg: args) {
        md.update(arg);
      }
      return md.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("unable to use hash algorithm: " + algorithm, e);
    }
  }

}
