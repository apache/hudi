/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.crypot.kms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.parquet.crypto.KeyAccessDeniedException;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.keytools.KeyToolkit;
import org.apache.parquet.crypto.keytools.KmsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * This is a mock class, built for testing only. Don't use it as an example of KmsClient implementation.
 * (org.apache.parquet.crypto.keytools.samples.VaultClient is the sample implementation).
 */
public class InMemoryKMS implements KmsClient {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryKMS.class);

  private static final ConfigProperty<String> KEY_LIST_PROPERTY_NAME = ConfigProperty
      .key("hoodie.parquet.encryption.key.list")
      .noDefaultValue()
      .withDocumentation("hudi parquet encryption key list, example: keyName_1:value_1, keyName_2:value_2");

  private static Map<String,byte[]> masterKeyMap;

  @Override
  public synchronized void initialize(Configuration configuration, String kmsInstanceID, String kmsInstanceURL, String accessToken) {
    // Parse master  keys
    String[] masterKeys = configuration.getTrimmedStrings(KEY_LIST_PROPERTY_NAME.key());
    if (null == masterKeys || masterKeys.length == 0) {
      throw new ParquetCryptoRuntimeException("No encryption key list");
    }
    masterKeyMap = parseKeyList(masterKeys);
  }

  private static Map<String, byte[]> parseKeyList(String[] masterKeys) {

    Map<String,byte[]> keyMap = new HashMap<>();

    int nKeys = masterKeys.length;
    for (int i = 0; i < nKeys; i++) {
      String[] parts = masterKeys[i].split(":");
      String keyName = parts[0].trim();
      if (parts.length != 2) {
        throw new IllegalArgumentException("Key '" + keyName + "' is not formatted correctly");
      }
      String key = parts[1].trim();
      try {
        byte[] keyBytes = Base64.getDecoder().decode(key);
        keyMap.put(keyName, keyBytes);
      } catch (IllegalArgumentException e) {
        LOG.warn("Could not decode key '" + keyName + "'!");
        throw e;
      }
    }
    return keyMap;
  }

  @Override
  public synchronized String wrapKey(byte[] keyBytes, String masterKeyIdentifier)
      throws KeyAccessDeniedException, UnsupportedOperationException {

    byte[] masterKey = masterKeyMap.get(masterKeyIdentifier);
    if (null == masterKey) {
      throw new ParquetCryptoRuntimeException("Key not found: " + masterKeyIdentifier);
    }
    byte[] add = masterKeyIdentifier.getBytes(StandardCharsets.UTF_8);
    return KeyToolkit.encryptKeyLocally(keyBytes, masterKey, add);
  }

  @Override
  public synchronized byte[] unwrapKey(String wrappedKey, String masterKeyIdentifier)
      throws KeyAccessDeniedException, UnsupportedOperationException {
    byte[] masterKey = masterKeyMap.get(masterKeyIdentifier);
    if (null == masterKey) {
      throw new ParquetCryptoRuntimeException("Key not found: " + masterKeyIdentifier);
    }
    byte[] add = masterKeyIdentifier.getBytes(StandardCharsets.UTF_8);
    return KeyToolkit.decryptKeyLocally(wrappedKey, masterKey, add);
  }
}