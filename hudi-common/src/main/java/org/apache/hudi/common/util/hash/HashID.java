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

package org.apache.hudi.common.util.hash;

import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.util.IOUtils;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * A stateless Hash class which generates ID for the desired bit count.
 */
public class HashID implements Serializable {

  private static final String MD5_ALGORITHM_NAME = "MD5";
  private static final int HASH_SEED = 0xdabadaba;

  /**
   * Represents HashID size in bits.
   */
  public enum Size {
    BITS_32(32),
    BITS_64(64),
    BITS_128(128);

    private final int bits;

    Size(int bitCount) {
      this.bits = bitCount;
    }

    /**
     * Get this Hash size in bytes.
     *
     * @return Bytes needed to represent this size
     */
    public int byteSize() {
      return (((this.bits - 1) / Byte.SIZE) + 1);
    }

    /**
     * Get this Hash size in bits.
     *
     * @return bits needed to represent the size
     */
    public int bits() {
      return this.bits;
    }

    @Override
    public String toString() {
      return "HashSize{" + bits + "}";
    }
  }

  /**
   * Get the hash value for a string message and for the desired @{@link HashID.Size}.
   *
   * @param message - String message to get the hash value for
   * @param bits    - @{@link HashID.Size} of the hash value
   * @return Hash value for the message as byte array
   */
  public static byte[] hash(final String message, final Size bits) {
    return hash(getUTF8Bytes(message), bits);
  }

  /**
   * Get the hash value for a byte array and for the desired @{@link Size}.
   *
   * @param messageBytes - Byte array message to get the hash value for
   * @param bits         - @{@link Size} of the hash value
   * @return Hash value for the message as byte array
   */
  public static byte[] hash(final byte[] messageBytes, final Size bits) {
    switch (bits) {
      case BITS_32:
      case BITS_64:
        return getXXHash(messageBytes, bits);
      case BITS_128:
        return getMD5Hash(messageBytes);
      default:
        throw new IllegalArgumentException("Unexpected Hash size bits: " + bits);
    }
  }

  /**
   * Get the hash value as string for a given string and for the desired @{@link Size}.
   *
   * @param input - String message to get the hash value for.
   * @param size  - @{@link Size} of the hash value
   * @return Hash value for the message as string.
   */
  public static String generateXXHashAsString(String input, Size size) {
    // Compute the hash
    byte[] hashBytes = hash(input, size);
    // Convert the hash value to a hexadecimal string
    StringBuilder hexString = new StringBuilder();
    for (byte hashByte : hashBytes) {
      hexString.append(String.format("%02X", hashByte));
    }
    return hexString.toString();
  }

  public static int getXXHash32(final String message, int hashSeed) {
    return getXXHash32(getUTF8Bytes(message), hashSeed);
  }

  public static int getXXHash32(final byte[] message, int hashSeed) {
    XXHashFactory factory = XXHashFactory.fastestInstance();
    return factory.hash32().hash(message, 0, message.length, hashSeed);
  }

  private static byte[] getXXHash(final byte[] message, final Size bits) {
    XXHashFactory factory = XXHashFactory.fastestInstance();
    switch (bits) {
      case BITS_32:
        XXHash32 hash32 = factory.hash32();
        return IOUtils.toBytes(hash32.hash(message, 0, message.length, HASH_SEED));
      case BITS_64:
        XXHash64 hash64 = factory.hash64();
        return IOUtils.toBytes(hash64.hash(message, 0, message.length, HASH_SEED));
      default:
        throw new HoodieIOException("XX" + bits + " hash is unsupported!");
    }
  }

  private static byte[] getMD5Hash(final byte[] message) throws HoodieIOException {
    try {
      MessageDigest messageDigest = MessageDigest.getInstance(MD5_ALGORITHM_NAME);
      messageDigest.update(message);
      return messageDigest.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new HoodieIOException("Failed to create MD5 Hash: " + e);
    }
  }
}
