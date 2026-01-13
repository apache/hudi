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

import org.apache.hudi.client.transaction.lock.StorageLockClient;
import org.apache.hudi.client.transaction.lock.models.LockGetResult;
import org.apache.hudi.client.transaction.lock.models.LockUpsertResult;
import org.apache.hudi.client.transaction.lock.models.StorageLockData;
import org.apache.hudi.client.transaction.lock.models.StorageLockFile;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieLockException;

import com.azure.core.credential.AzureSasCredential;
import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * ADLS Gen2 (ABFS/ABFSS) implementation of {@link StorageLockClient} using Azure Blob conditional requests.
 *
 * <p>Lock semantics match S3/GCS storage lock clients:
 * - Create: conditional write with If-None-Match: *
 * - Update/Renew/Expire: conditional write with If-Match: &lt;etag&gt;
 *
 * <p>Expected lock URI format:
 * <ul>
 *   <li>{@code abfs://&lt;container&gt;@&lt;account&gt;.dfs.core.windows.net/&lt;path&gt;}</li>
 *   <li>{@code abfss://&lt;container&gt;@&lt;account&gt;.dfs.core.windows.net/&lt;path&gt;}</li>
 * </ul>
 *
 * <p>Authentication precedence (via {@link Properties}):
 * <ul>
 *   <li>{@code hoodie.write.lock.azure.connection.string}</li>
 *   <li>{@code hoodie.write.lock.azure.sas.token}</li>
 *   <li>DefaultAzureCredential</li>
 * </ul>
 */
@Slf4j
@ThreadSafe
public class ADLSGen2StorageLockClient implements StorageLockClient {

  private static final int PRECONDITION_FAILURE_ERROR_CODE = 412;
  private static final int NOT_FOUND_ERROR_CODE = 404;
  private static final int CONFLICT_ERROR_CODE = 409;
  private static final int RATE_LIMIT_ERROR_CODE = 429;
  private static final int INTERNAL_SERVER_ERROR_CODE_MIN = 500;

  public static final String AZURE_CONNECTION_STRING = "hoodie.write.lock.azure.connection.string";
  public static final String AZURE_SAS_TOKEN = "hoodie.write.lock.azure.sas.token";

  private final Logger logger;
  private final BlobServiceClient blobServiceClient;
  private final BlobClient lockBlobClient;
  private final Properties clientProperties;
  private final String ownerId;
  private final String lockFileUri;

  /**
   * Constructor used by reflection by {@link org.apache.hudi.client.transaction.lock.StorageBasedLockProvider}.
   *
   * @param ownerId     lock owner id
   * @param lockFileUri lock file URI (ABFS/ABFSS)
   * @param props       properties used to customize/authenticate the Azure client
   */
  public ADLSGen2StorageLockClient(String ownerId, String lockFileUri, Properties props) {
    this(ownerId, lockFileUri, props, createDefaultBlobServiceClient(), log);
  }

  @VisibleForTesting
  ADLSGen2StorageLockClient(
      String ownerId,
      String lockFileUri,
      Properties props,
      Functions.Function1<AzureLocation, BlobServiceClient> blobServiceClientSupplier,
      Logger logger) {
    this.ownerId = ownerId;
    this.lockFileUri = lockFileUri;
    this.logger = logger;
    this.clientProperties = props;

    AzureLocation location = parseAzureLocation(lockFileUri);
    this.blobServiceClient = blobServiceClientSupplier.apply(location.withProperties(props));
    BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(location.container);
    this.lockBlobClient = containerClient.getBlobClient(location.blobPath);
  }

  private static Functions.Function1<AzureLocation, BlobServiceClient> createDefaultBlobServiceClient() {
    return (location) -> {
      Properties props = location.props;
      BlobServiceClientBuilder builder = new BlobServiceClientBuilder();

      String connectionString = props == null ? null : props.getProperty(AZURE_CONNECTION_STRING);
      if (connectionString != null && !connectionString.trim().isEmpty()) {
        return builder.connectionString(connectionString).buildClient();
      }

      builder.endpoint(location.blobEndpoint);
      String sasToken = props == null ? null : props.getProperty(AZURE_SAS_TOKEN);
      if (sasToken != null && !sasToken.trim().isEmpty()) {
        String cleaned = sasToken.startsWith("?") ? sasToken.substring(1) : sasToken;
        return builder.credential(new AzureSasCredential(cleaned)).buildClient();
      }

      return builder.credential(new DefaultAzureCredentialBuilder().build()).buildClient();
    };
  }

  @Override
  public Pair<LockUpsertResult, Option<StorageLockFile>> tryUpsertLockFile(
      StorageLockData newLockData,
      Option<StorageLockFile> previousLockFile) {
    String expectedEtag = previousLockFile.isPresent() ? previousLockFile.get().getVersionId() : null;
    try {
      StorageLockFile updated = createOrUpdateLockFileInternal(newLockData, expectedEtag);
      return Pair.of(LockUpsertResult.SUCCESS, Option.of(updated));
    } catch (BlobStorageException e) {
      int code = e.getStatusCode();
      if (code == PRECONDITION_FAILURE_ERROR_CODE || code == CONFLICT_ERROR_CODE) {
        logger.info("OwnerId: {}, Unable to write new lock file. Another process has modified this lockfile {} already.",
            ownerId, lockFileUri);
        return Pair.of(LockUpsertResult.ACQUIRED_BY_OTHERS, Option.empty());
      } else if (code == RATE_LIMIT_ERROR_CODE) {
        logger.warn("OwnerId: {}, Rate limit exceeded for lock file: {}", ownerId, lockFileUri);
      } else if (code >= INTERNAL_SERVER_ERROR_CODE_MIN) {
        logger.warn("OwnerId: {}, Azure returned internal server error code for lock file: {}",
            ownerId, lockFileUri, e);
      } else {
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
      BlobProperties props = lockBlobClient.getProperties();
      String eTag = props.getETag();
      try {
        return getLockFileFromBlob(lockBlobClient, eTag);
      } catch (BlobStorageException e) {
        // Blob can change/disappear after properties call; treat 404 during stream read as UNKNOWN_ERROR.
        return Pair.of(handleGetStorageException(e, true), Option.empty());
      }
    } catch (BlobStorageException e) {
      return Pair.of(handleGetStorageException(e, false), Option.empty());
    }
  }

  private LockGetResult handleGetStorageException(BlobStorageException e, boolean ignore404) {
    int code = e.getStatusCode();
    if (code == NOT_FOUND_ERROR_CODE) {
      if (ignore404) {
        logger.info("OwnerId: {}, Azure stream read failure detected: {}", ownerId, lockFileUri);
      } else {
        logger.info("OwnerId: {}, Object not found in the path: {}", ownerId, lockFileUri);
        return LockGetResult.NOT_EXISTS;
      }
    } else if (code == RATE_LIMIT_ERROR_CODE) {
      logger.warn("OwnerId: {}, Rate limit exceeded for lock file: {}", ownerId, lockFileUri);
    } else if (code >= INTERNAL_SERVER_ERROR_CODE_MIN) {
      logger.warn("OwnerId: {}, Azure returned internal server error code for lock file: {}", ownerId, lockFileUri, e);
    } else {
      throw e;
    }
    return LockGetResult.UNKNOWN_ERROR;
  }

  private @Nonnull Pair<LockGetResult, Option<StorageLockFile>> getLockFileFromBlob(BlobClient blobClient, String eTag) {
    try (InputStream inputStream = blobClient.openInputStream()) {
      return Pair.of(LockGetResult.SUCCESS, Option.of(StorageLockFile.createFromStream(inputStream, eTag)));
    } catch (IOException ioe) {
      throw new UncheckedIOException("Failed reading blob: " + lockFileUri, ioe);
    }
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
    lockBlobClient.uploadWithResponse(options, null, Context.NONE);
    String newEtag = lockBlobClient.getProperties().getETag();
    return new StorageLockFile(lockData, newEtag);
  }

  @Override
  public Option<String> readObject(String filePath, boolean checkExistsFirst) {
    try {
      AzureLocation location = parseAzureLocation(filePath);
      AzureLocation lockLocation = parseAzureLocation(lockFileUri);
      BlobServiceClient svc = location.blobEndpoint.equals(lockLocation.blobEndpoint)
          ? blobServiceClient
          : createDefaultBlobServiceClient().apply(location.withProperties(clientProperties));
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
      AzureLocation lockLocation = parseAzureLocation(lockFileUri);
      BlobServiceClient svc = location.blobEndpoint.equals(lockLocation.blobEndpoint)
          ? blobServiceClient
          : createDefaultBlobServiceClient().apply(location.withProperties(clientProperties));
      BlobClient blobClient = svc.getBlobContainerClient(location.container).getBlobClient(location.blobPath);
      blobClient.upload(BinaryData.fromString(content), true);
      logger.debug("Successfully wrote object to: {}", filePath);
      return true;
    } catch (Exception e) {
      logger.error("Error writing object to: {}", filePath, e);
      return false;
    }
  }

  @Override
  public void close() {
    // BlobServiceClient does not require explicit close. No-op.
  }

  private static AzureLocation parseAzureLocation(String uriString) {
    try {
      URI uri = new URI(uriString);
      String scheme = uri.getScheme();
      if (scheme == null) {
        throw new IllegalArgumentException("URI does not contain a valid scheme.");
      }

      String authority = uri.getAuthority();
      String path = uri.getPath() == null ? "" : uri.getPath().replaceFirst("/", "");

      if ("abfs".equalsIgnoreCase(scheme) || "abfss".equalsIgnoreCase(scheme)) {
        // abfs[s]://<container>@<account>.dfs.core.windows.net/<path>
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

      if ("https".equalsIgnoreCase(scheme)) {
        // https://<account>.blob.core.windows.net/<container>/<path>
        if (authority == null || authority.isEmpty()) {
          throw new IllegalArgumentException("HTTPS URI authority missing: " + uriString);
        }
        int slash = path.indexOf('/');
        if (slash <= 0 || slash == path.length() - 1) {
          throw new IllegalArgumentException("HTTPS URI must be in the form 'https://<host>/<container>/<path>': " + uriString);
        }
        String container = path.substring(0, slash);
        String blobPath = path.substring(slash + 1);
        return new AzureLocation("https://" + authority, container, blobPath, null);
      }

      throw new IllegalArgumentException("Unsupported scheme for Azure storage lock: " + scheme);
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
  static final class AzureLocation {
    final String blobEndpoint;
    final String container;
    final String blobPath;
    final Properties props;

    AzureLocation(String blobEndpoint, String container, String blobPath, Properties props) {
      this.blobEndpoint = blobEndpoint;
      this.container = container;
      this.blobPath = blobPath;
      this.props = props;
    }

    AzureLocation withProperties(Properties props) {
      return new AzureLocation(blobEndpoint, container, blobPath, props);
    }
  }
}
