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

package org.apache.hudi.azure.transaction.lock;

import org.apache.hudi.azure.credentials.AzureCredentialFactory;
import org.apache.hudi.client.transaction.lock.StorageLockClient;
import org.apache.hudi.client.transaction.lock.models.LockGetResult;
import org.apache.hudi.client.transaction.lock.models.LockUpsertResult;
import org.apache.hudi.client.transaction.lock.models.StorageLockData;
import org.apache.hudi.client.transaction.lock.models.StorageLockFile;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.AzureStorageLockConfig;
import org.apache.hudi.config.StorageBasedLockConfig;
import org.apache.hudi.exception.HoodieLockException;

import com.azure.core.credential.AzureSasCredential;
import com.azure.core.exception.HttpResponseException;
import com.azure.core.http.policy.ExponentialBackoffOptions;
import com.azure.core.http.policy.RetryOptions;
import com.azure.core.http.rest.Response;
import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.core.util.HttpClientOptions;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hudi.common.util.ConfigUtils.getLongWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;

/**
 * Azure Storage implementation of {@link StorageLockClient} using Azure Blob conditional requests.
 *
 * <p>Supports the following URI schemes:
 * <ul>
 *   <li>ADLS Gen2: {@code abfs://} and {@code abfss://}</li>
 *   <li>Azure Blob Storage: {@code wasb://} and {@code wasbs://}</li>
 * </ul>
 *
 *
 * <ul>
 *   <li>Create: conditional write with If-None-Match: *</li>
 *   <li>Update/Renew/Expire: conditional write with If-Match: &lt;etag&gt;</li>
 * </ul>
 *
 * <p>Expected lock URI formats:
 * <ul>
 *   <li>{@code abfs://&lt;container&gt;@&lt;account&gt;.dfs.core.windows.net/&lt;path&gt;}</li>
 *   <li>{@code abfss://&lt;container&gt;@&lt;account&gt;.dfs.core.windows.net/&lt;path&gt;}</li>
 *   <li>{@code wasb://&lt;container&gt;@&lt;account&gt;.blob.core.windows.net/&lt;path&gt;}</li>
 *   <li>{@code wasbs://&lt;container&gt;@&lt;account&gt;.blob.core.windows.net/&lt;path&gt;}</li>
 * </ul>
 *
 * <p>Authentication precedence (via {@link Properties}):
 * <ol>
 *   <li>{@link AzureStorageLockConfig#AZURE_CONNECTION_STRING} — connection string (includes shared key)</li>
 *   <li>{@link AzureStorageLockConfig#AZURE_SAS_TOKEN} — shared access signature</li>
 *   <li>{@link AzureStorageLockConfig#AZURE_MANAGED_IDENTITY_CLIENT_ID} — user-assigned managed identity
 *       via {@code ManagedIdentityCredential}</li>
 *   <li>{@link AzureStorageLockConfig#AZURE_CLIENT_TENANT_ID} +
 *       {@link AzureStorageLockConfig#AZURE_CLIENT_ID} +
 *       {@link AzureStorageLockConfig#AZURE_CLIENT_SECRET} — service principal
 *       via {@code ClientSecretCredential}</li>
 *   <li>{@code DefaultAzureCredential} — probing chain; see {@link org.apache.hudi.azure.credentials.AzureCredentialFactory}</li>
 * </ol>
 */
@Slf4j
@ThreadSafe
public class AzureStorageLockClient implements StorageLockClient {

  private static final int PRECONDITION_FAILURE_ERROR_CODE = 412;
  private static final int NOT_FOUND_ERROR_CODE = 404;
  private static final int CONFLICT_ERROR_CODE = 409;
  private static final int RATE_LIMIT_ERROR_CODE = 429;
  private static final int INTERNAL_SERVER_ERROR_CODE_MIN = 500;

  private final Logger logger;
  private final BlobServiceClient blobServiceClient;
  private final Functions.Function1<AzureLocation, BlobServiceClient> blobServiceClientSupplier;
  private final ConcurrentMap<String, BlobServiceClient> secondaryBlobServiceClients;
  private final BlobClient lockBlobClient;
  private final Properties clientProperties;
  private final String ownerId;
  private final String lockFileUri;
  private final String lockBlobEndpoint;

  /**
   * Constructor used by reflection by {@link org.apache.hudi.client.transaction.lock.StorageBasedLockProvider}.
   *
   * @param ownerId     lock owner id
   * @param lockFileUri lock file URI (abfs/abfss/wasb/wasbs)
   * @param props       properties used to customize/authenticate the Azure client
   */
  public AzureStorageLockClient(String ownerId, String lockFileUri, Properties props) {
    this(ownerId, lockFileUri, props, createDefaultBlobServiceClient(), LoggerFactory.getLogger(AzureStorageLockClient.class));
  }

  @VisibleForTesting
  AzureStorageLockClient(
      String ownerId,
      String lockFileUri,
      Properties props,
      Functions.Function1<AzureLocation, BlobServiceClient> blobServiceClientSupplier,
      Logger logger) {
    this.ownerId = ownerId;
    this.lockFileUri = lockFileUri;
    this.logger = logger;
    this.clientProperties = props;
    this.blobServiceClientSupplier = blobServiceClientSupplier;
    this.secondaryBlobServiceClients = new ConcurrentHashMap<>();

    AzureLocation location = parseAzureLocation(lockFileUri).withProperties(props);
    this.lockBlobEndpoint = location.blobEndpoint;
    this.blobServiceClient = blobServiceClientSupplier.apply(location);
    BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(location.container);
    this.lockBlobClient = containerClient.getBlobClient(location.blobPath);
  }

  private static Functions.Function1<AzureLocation, BlobServiceClient> createDefaultBlobServiceClient() {
    return (location) -> {
      Properties props = location.props;
      BlobServiceClientBuilder builder = new BlobServiceClientBuilder();
      configureAzureClientOptions(builder, props);

      // 1. Connection string (includes shared-key auth).
      String connectionString = getStringWithAltKeys(props, AzureStorageLockConfig.AZURE_CONNECTION_STRING, true);
      if (connectionString != null && !connectionString.trim().isEmpty()) {
        return builder.connectionString(connectionString).buildClient();
      }

      builder.endpoint(location.blobEndpoint);

      // 2. SAS token.
      String sasToken = getStringWithAltKeys(props, AzureStorageLockConfig.AZURE_SAS_TOKEN, true);
      if (sasToken != null && !sasToken.trim().isEmpty()) {
        String cleaned = sasToken.startsWith("?") ? sasToken.substring(1) : sasToken;
        return builder.credential(new AzureSasCredential(cleaned)).buildClient();
      }

      // 3. TokenCredential — MI, service principal, or DefaultAzureCredential fallback.
      return builder.credential(AzureCredentialFactory.getAzureCredential(props)).buildClient();
    };
  }

  private static void configureAzureClientOptions(BlobServiceClientBuilder builder, Properties props) {
    // Set Azure SDK timeouts based on lock validity to avoid long-hanging calls.
    TypedProperties typedProps = new TypedProperties();
    if (props != null) {
      typedProps.putAll(props);
    }
    long validityTimeoutSecs;
    try {
      validityTimeoutSecs = getLongWithAltKeys(typedProps, StorageBasedLockConfig.VALIDITY_TIMEOUT_SECONDS);
    } catch (NumberFormatException e) {
      log.warn("Invalid format for config '{}', falling back to default value {}",
          StorageBasedLockConfig.VALIDITY_TIMEOUT_SECONDS.key(),
          StorageBasedLockConfig.VALIDITY_TIMEOUT_SECONDS.defaultValue(), e);
      validityTimeoutSecs = StorageBasedLockConfig.VALIDITY_TIMEOUT_SECONDS.defaultValue();
    }
    long azureCallTimeoutSecs = Math.max(1, validityTimeoutSecs / 5);

    // Disable automatic SDK retries; Hudi manages retries at the lock-provider level.
    ExponentialBackoffOptions exponentialOptions = new ExponentialBackoffOptions().setMaxRetries(0);
    RetryOptions retryOptions = new RetryOptions(exponentialOptions);

    HttpClientOptions clientOptions = new HttpClientOptions()
        .setResponseTimeout(Duration.ofSeconds(azureCallTimeoutSecs))
        .setReadTimeout(Duration.ofSeconds(azureCallTimeoutSecs));

    builder.retryOptions(retryOptions).clientOptions(clientOptions);
  }

  @Override
  public Pair<LockUpsertResult, Option<StorageLockFile>> tryUpsertLockFile(
      StorageLockData newLockData,
      Option<StorageLockFile> previousLockFile) {
    String expectedEtag = previousLockFile.isPresent()
        ? previousLockFile.get().getVersionId()
        : null;
    try {
      StorageLockFile updated = createOrUpdateLockFileInternal(newLockData, expectedEtag);
      return Pair.of(LockUpsertResult.SUCCESS, Option.of(updated));
    } catch (BlobStorageException e) {
      return Pair.of(handleUpsertBlobStorageException(e), Option.empty());
    } catch (HttpResponseException e) {
      logger.error("OwnerId: {}, Unexpected Azure SDK error while writing lock file: {}",
          ownerId, lockFileUri, e);
      if (!previousLockFile.isPresent()) {
        // For create, fail fast since this indicates a larger issue.
        throw e;
      }
      return Pair.of(LockUpsertResult.UNKNOWN_ERROR, Option.empty());
    } catch (Exception e) {
      logger.error("OwnerId: {}, Unexpected error while writing lock file: {}", ownerId, lockFileUri, e);
      return Pair.of(LockUpsertResult.UNKNOWN_ERROR, Option.empty());
    }
  }

  @Override
  public Pair<LockGetResult, Option<StorageLockFile>> readCurrentLockFile() {
    try {
      Response<BinaryData> response = lockBlobClient.downloadContentWithResponse(null, null, null, Context.NONE);
      //Check for null or empty ETag and inconsistent quotes
      String eTag = canonicalizeEtag(
          response.getHeaders() != null ? response.getHeaders().getValue("ETag") : null,
          "download");
      StorageLockFile lockFile = StorageLockFile.createFromStream(response.getValue().toStream(), eTag);
      return Pair.of(LockGetResult.SUCCESS, Option.of(lockFile));
    } catch (BlobStorageException e) {
      return Pair.of(handleGetStorageException(e), Option.empty());
    }
  }

  private LockGetResult handleGetStorageException(BlobStorageException e) {
    int code = e.getStatusCode();
    if (code == NOT_FOUND_ERROR_CODE || e.getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND) {
      logger.info("OwnerId: {}, Object not found in the path: {}", ownerId, lockFileUri);
      return LockGetResult.NOT_EXISTS;
    } else if (code == RATE_LIMIT_ERROR_CODE) {
      logger.warn("OwnerId: {}, Rate limit exceeded for lock file: {}", ownerId, lockFileUri);
    } else if (code >= INTERNAL_SERVER_ERROR_CODE_MIN) {
      logger.warn("OwnerId: {}, Azure returned internal server error code for lock file: {}", ownerId, lockFileUri, e);
    } else {
      throw e;
    }
    return LockGetResult.UNKNOWN_ERROR;
  }

  private StorageLockFile createOrUpdateLockFileInternal(StorageLockData lockData, String expectedEtag) {
    byte[] bytes = StorageLockFile.toByteArray(lockData);
    BlobRequestConditions conditions = new BlobRequestConditions();
    if (expectedEtag == null) {
      conditions.setIfNoneMatch("*");
    } else {
      conditions.setIfMatch(expectedEtag);
    }

    BlobParallelUploadOptions options = new BlobParallelUploadOptions(BinaryData.fromBytes(bytes))
        .setRequestConditions(conditions);
    Response<BlockBlobItem> response = lockBlobClient.uploadWithResponse(options, null, Context.NONE);
    String newEtag = response.getHeaders() != null ? response.getHeaders().getValue("ETag") : null;
    if (newEtag == null && response.getValue() != null) {
      newEtag = response.getValue().getETag();
    }
    //Check for null or empty ETag and inconsistent quotes
    newEtag = canonicalizeEtag(newEtag, "upload");
    return new StorageLockFile(lockData, newEtag);
  }

  private String canonicalizeEtag(String eTag, String operation) {
    if (eTag == null) {
      throw new HoodieLockException("Missing ETag in Azure " + operation + " response for lock file: " + lockFileUri);
    }

    String normalized = eTag.trim();
    if (normalized.isEmpty()) {
      throw new HoodieLockException("Missing ETag in Azure " + operation + " response for lock file: " + lockFileUri);
    }

    boolean startsWithQuote = normalized.startsWith("\"");
    boolean endsWithQuote = normalized.endsWith("\"");
    if (startsWithQuote && endsWithQuote) {
      return normalized;
    }
    if (!startsWithQuote && !endsWithQuote) {
      return "\"" + normalized + "\"";
    }

    throw new HoodieLockException("Malformed ETag in Azure " + operation + " response for lock file: " + lockFileUri);
  }

  private LockUpsertResult handleUpsertBlobStorageException(BlobStorageException e) {
    int code = e.getStatusCode();
    if (code == PRECONDITION_FAILURE_ERROR_CODE || e.getErrorCode() == BlobErrorCode.CONDITION_NOT_MET) {
      logger.info("OwnerId: {}, Unable to write new lock file. Another process has modified this lockfile {} already.",
          ownerId, lockFileUri);
      return LockUpsertResult.ACQUIRED_BY_OTHERS;
    } else if (code == CONFLICT_ERROR_CODE) {
      logger.info("OwnerId: {}, Retriable conditional request conflict error: {}", ownerId, lockFileUri);
    } else if (code == RATE_LIMIT_ERROR_CODE) {
      logger.warn("OwnerId: {}, Rate limit exceeded for lock file: {}", ownerId, lockFileUri);
    } else if (code >= INTERNAL_SERVER_ERROR_CODE_MIN) {
      logger.warn("OwnerId: {}, Azure returned internal server error code for lock file: {}", ownerId, lockFileUri, e);
    } else {
      logger.warn("OwnerId: {}, Error writing lock file: {}", ownerId, lockFileUri, e);
    }
    return LockUpsertResult.UNKNOWN_ERROR;
  }

  @Override
  public Option<String> readObject(String filePath, boolean checkExistsFirst) {
    try {
      AzureLocation location = parseAzureLocation(filePath);
      BlobServiceClient svc = getBlobServiceClient(location);
      BlobClient blobClient = svc.getBlobContainerClient(location.container).getBlobClient(location.blobPath);

      if (checkExistsFirst && !blobClient.exists()) {
        logger.debug("JSON config file not found: {}", filePath);
        return Option.empty();
      }
      byte[] bytes = blobClient.downloadContent().toBytes();
      return Option.of(new String(bytes, UTF_8));
    } catch (BlobStorageException e) {
      if (e.getStatusCode() == NOT_FOUND_ERROR_CODE) {
        logger.debug("JSON config file not found: {}", filePath);
      } else {
        logger.warn("Error reading JSON config file: {}", filePath, e);
      }
      return Option.empty();
    } catch (Exception e) {
      logger.warn("Error reading JSON config file: {}", filePath, e);
      return Option.empty();
    }
  }

  @Override
  public boolean writeObject(String filePath, String content) {
    try {
      AzureLocation location = parseAzureLocation(filePath);
      BlobServiceClient svc = getBlobServiceClient(location);
      BlobClient blobClient = svc.getBlobContainerClient(location.container).getBlobClient(location.blobPath);
      blobClient.upload(BinaryData.fromString(content), true);
      logger.debug("Successfully wrote object to: {}", filePath);
      return true;
    } catch (Exception e) {
      logger.error("Error writing object to: {}", filePath, e);
      return false;
    }
  }

  private BlobServiceClient getBlobServiceClient(AzureLocation location) {
    if (location.blobEndpoint.equals(lockBlobEndpoint)) {
      return blobServiceClient;
    }
    return secondaryBlobServiceClients.computeIfAbsent(
        location.blobEndpoint,
        endpoint -> blobServiceClientSupplier.apply(location.withProperties(clientProperties)));
  }

  @Override
  public void close() {
    // BlobServiceClient does not require explicit close. No-op.
  }

  @VisibleForTesting
  static AzureLocation parseAzureLocation(String uriString) {
    try {
      URI uri = new URI(uriString);
      String scheme = uri.getScheme();
      if (scheme == null) {
        throw new IllegalArgumentException("URI does not contain a valid scheme.");
      }

      String authority = uri.getAuthority();
      String path = uri.getPath() == null ? "" : uri.getPath().replaceFirst("/", "");

      // ADLS Gen2: abfs[s]://<container>@<account>.dfs.core.windows.net/<path>
      if ("abfs".equalsIgnoreCase(scheme) || "abfss".equalsIgnoreCase(scheme)) {
        if (authority == null || !authority.contains("@")) {
          throw new IllegalArgumentException("ABFS URI authority must be in the form '<container>@<host>': " + uriString);
        }
        String[] parts = authority.split("@", 2);
        String container = parts[0];
        String host = parts[1];
        String endpointHost = dfsHostToBlobHost(host);
        String endpoint = "https://" + endpointHost;
        if (container.isEmpty() || path.isEmpty()) {
          throw new IllegalArgumentException("ABFS URI must contain container and path: " + uriString);
        }
        return new AzureLocation(endpoint, container, path, null);
      }

      // Azure Blob Storage: wasb[s]://<container>@<account>.blob.core.windows.net/<path>
      if ("wasb".equalsIgnoreCase(scheme) || "wasbs".equalsIgnoreCase(scheme)) {
        if (authority == null || !authority.contains("@")) {
          throw new IllegalArgumentException("WASB URI authority must be in the form '<container>@<host>': " + uriString);
        }
        String[] parts = authority.split("@", 2);
        String container = parts[0];
        String host = parts[1];
        String endpoint = "https://" + host;
        if (container.isEmpty() || path.isEmpty()) {
          throw new IllegalArgumentException("WASB URI must contain container and path: " + uriString);
        }
        return new AzureLocation(endpoint, container, path, null);
      }

      // Direct HTTP(S) blob URL.
      if ("https".equalsIgnoreCase(scheme) || "http".equalsIgnoreCase(scheme)) {
        BlobUrlParts parts = BlobUrlParts.parse(uriString);
        String container = parts.getBlobContainerName();
        String blobPath = parts.getBlobName();
        if (container == null || container.isEmpty() || blobPath == null || blobPath.isEmpty()) {
          throw new IllegalArgumentException("HTTP(S) URI must contain container and path: " + uriString);
        }
        return new AzureLocation(parts.getScheme() + "://" + parts.getHost(), container, blobPath, null);
      }

      throw new IllegalArgumentException("Unsupported scheme for Azure storage lock: " + scheme
          + ". Supported schemes: abfs, abfss, wasb, wasbs, https, http");
    } catch (URISyntaxException e) {
      throw new HoodieLockException("Failed to parse Azure URI: " + uriString, e);
    }
  }

  private static String dfsHostToBlobHost(String host) {
    if (host == null) {
      return null;
    }
    if (host.endsWith(".dfs.core.windows.net")) {
      return host.replace(".dfs.core.windows.net", ".blob.core.windows.net");
    }
    return host;
  }
 
  @VisibleForTesting
  @Getter
  @AllArgsConstructor
  static final class AzureLocation {
    final String blobEndpoint;
    final String container;
    final String blobPath;
    final Properties props;

    AzureLocation withProperties(Properties props) {
      return new AzureLocation(blobEndpoint, container, blobPath, props);
    }
  }
}
