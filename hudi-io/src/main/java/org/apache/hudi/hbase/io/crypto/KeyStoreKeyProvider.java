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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Locale;
import java.util.Properties;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A basic KeyProvider that can resolve keys from a protected KeyStore file
 * on the local filesystem. It is configured with a URI passed in as a String
 * to init(). The URI should have the form:
 * <p>
 * <pre>    scheme://path?option1=value1&amp;option2=value2</pre>
 * <p>
 * <i>scheme</i> can be either "jks" or "jceks", specifying the file based
 * providers shipped with every JRE. The latter is the certificate store for
 * the SunJCE cryptography extension, or PKCS #12, and is capable of storing
 * SecretKeys.
 * <p>
 * <i>path</i> is the location of the keystore in the filesystem namespace.
 * <p>
 * Options can be specified as query parameters.
 * <p>
 * If the store was created with a password, the password can be specified
 * using the option 'password'.
 * <p>
 * For example:
 * <p>
 * <pre>    jceks:///var/tmp/example.ks?password=foobar</pre>
 * <p>
 * It is assumed that all keys in the store are protected with the same
 * password.
 * <p>
 * Alternatively, a properties file can be specified containing passwords for
 * keys in the keystore.
 * <pre>    jceks:///var/tmp/example.ks?passwordFile=/var/tmp/example.pw</pre>
 * <p>
 * Subclasses for supporting KeyStores that are not file based can extend the
 * protected methods of this class to specify the appropriate
 * LoadStoreParameters.
 */
@InterfaceAudience.Public
public class KeyStoreKeyProvider implements KeyProvider {

  protected KeyStore store;
  protected char[] password;         // can be null if no password
  protected Properties passwordFile; // can be null if no file provided

  protected void processParameter(String name, String value) throws IOException {
    if (name.equalsIgnoreCase(KeyProvider.PASSWORD)) {
      password = value.toCharArray();
    }
    if (name.equalsIgnoreCase(KeyProvider.PASSWORDFILE)) {
      Properties p = new Properties();
      InputStream in = new BufferedInputStream(new FileInputStream(new File(value)));
      try {
        p.load(in);
        passwordFile = p;
      } finally {
        in.close();
      }
    }
  }

  protected void processParameters(URI uri) throws IOException {
    String params = uri.getQuery();
    if (params == null || params.isEmpty()) {
      return;
    }
    do {
      int nameStart = 0;
      int nameEnd = params.indexOf('=');
      if (nameEnd == -1) {
        throw new RuntimeException("Invalid parameters: '" + params + "'");
      }
      int valueStart = nameEnd + 1;
      int valueEnd = params.indexOf('&');
      if (valueEnd == -1) {
        valueEnd = params.length();
      }
      String name = URLDecoder.decode(params.substring(nameStart, nameEnd), "UTF-8");
      String value = URLDecoder.decode(params.substring(valueStart, valueEnd), "UTF-8");
      processParameter(name, value);
      params = params.substring(valueEnd, params.length());
    } while (!params.isEmpty());
  }

  protected void load(URI uri) throws IOException {
    String path = uri.getPath();
    if (path == null || path.isEmpty()) {
      throw new RuntimeException("KeyProvider parameters should specify a path");
    }
    InputStream is = new FileInputStream(new File(path));
    try {
      store.load(is, password);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    } catch (CertificateException e) {
      throw new RuntimeException(e);
    } finally {
      is.close();
    }
  }

  @Override
  public void init(String params) {
    try {
      URI uri = new URI(params);
      String storeType = uri.getScheme();
      if (storeType == null || storeType.isEmpty()) {
        throw new RuntimeException("KeyProvider scheme should specify KeyStore type");
      }
      // KeyStore expects instance type specifications in uppercase
      store = KeyStore.getInstance(storeType.toUpperCase(Locale.ROOT));
      processParameters(uri);
      load(uri);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    } catch (KeyStoreException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected char[] getAliasPassword(String alias) {
    if (password != null) {
      return password;
    }
    if (passwordFile != null) {
      String p = passwordFile.getProperty(alias);
      if (p != null) {
        return p.toCharArray();
      }
    }
    return null;
  }

  @Override
  public Key getKey(String alias) {
    try {
      return store.getKey(alias, getAliasPassword(alias));
    } catch (UnrecoverableKeyException e) {
      throw new RuntimeException(e);
    } catch (KeyStoreException e) {
      throw new RuntimeException(e);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Key[] getKeys(String[] aliases) {
    Key[] result = new Key[aliases.length];
    for (int i = 0; i < aliases.length; i++) {
      result[i] = getKey(aliases[i]);
    }
    return result;
  }

}
