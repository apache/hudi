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

package org.apache.hudi.azure.credentials;

import org.apache.hudi.config.AzureStorageLockConfig;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;

import java.util.Properties;

import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;

/**
 * Factory for resolving an Azure {@link TokenCredential} from Hudi properties.
 *
 * <p>Credential precedence:
 * <ol>
 *   <li>User-assigned managed identity ({@link AzureStorageLockConfig#AZURE_MANAGED_IDENTITY_CLIENT_ID})
 *       — uses {@code ManagedIdentityCredential}</li>
 *   <li>Service principal ({@link AzureStorageLockConfig#AZURE_CLIENT_TENANT_ID} +
 *       {@link AzureStorageLockConfig#AZURE_CLIENT_ID} +
 *       {@link AzureStorageLockConfig#AZURE_CLIENT_SECRET})
 *       — uses {@code ClientSecretCredential}</li>
 *   <li>{@code DefaultAzureCredential} — (system-assigned MI,
 *       workload identity, env-var SP, Azure CLI, etc.); suitable for dev and environments
 *       where auth is controlled externally</li>
 * </ol>
 *
 * <p>Note: connection string and SAS token auth are not {@link TokenCredential}-based and are
 * handled directly by the caller before this factory is consulted.
 */
public class AzureCredentialFactory {

  /**
   * Lazily initializes {@code DefaultAzureCredential} on first use of the default chain only.
   */
  private static final class DefaultAzureCredentialHolder {
    static final TokenCredential INSTANCE = new DefaultAzureCredentialBuilder().build();
  }

  private AzureCredentialFactory() {
  }

  /**
   * Returns a {@link TokenCredential} resolved from the supplied properties.
   *
   * @param props Hudi lock properties
   * @return resolved credential, never {@code null}
   */
  public static TokenCredential getAzureCredential(Properties props) {
    if (props != null) {
      String miClientId = getStringWithAltKeys(props, AzureStorageLockConfig.AZURE_MANAGED_IDENTITY_CLIENT_ID, true);
      if (miClientId != null && !miClientId.trim().isEmpty()) {
        return new ManagedIdentityCredentialBuilder()
            .clientId(miClientId)
            .build();
      }

      String tenantId = getStringWithAltKeys(props, AzureStorageLockConfig.AZURE_CLIENT_TENANT_ID, true);
      String clientId = getStringWithAltKeys(props, AzureStorageLockConfig.AZURE_CLIENT_ID, true);
      String clientSecret = getStringWithAltKeys(props, AzureStorageLockConfig.AZURE_CLIENT_SECRET, true);
      if (tenantId != null && !tenantId.trim().isEmpty()
          && clientId != null && !clientId.trim().isEmpty()
          && clientSecret != null && !clientSecret.trim().isEmpty()) {
        return new ClientSecretCredentialBuilder()
            .tenantId(tenantId)
            .clientId(clientId)
            .clientSecret(clientSecret)
            .build();
      }
    }

    return DefaultAzureCredentialHolder.INSTANCE;
  }
}
