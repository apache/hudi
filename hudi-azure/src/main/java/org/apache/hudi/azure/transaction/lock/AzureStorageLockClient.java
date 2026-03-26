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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 * See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.hudi.azure.transaction.lock;

import org.apache.hudi.azure.utils.AzureStorageUtils;
import org.apache.hudi.azure.utils.AzureStorageUtils.AzureStorageUriComponents;
import org.apache.hudi.client.transaction.lock.StorageLockClient;
import org.apache.hudi.client.transaction.lock.models.LockGetResult;
import org.apache.hudi.client.transaction.lock.models.LockUpsertResult;
import org.apache.hudi.client.transaction.lock.models.StorageLockData;
import org.apache.hudi.client.transaction.lock.models.StorageLockFile;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.StorageBasedLockConfig;

import com.azure.core.exception.HttpResponseException;
import com.azure.core.http.rest.Response;
import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.core.util.HttpClientOptions;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobDownloadContentResponse;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.time.Duration;
import java.util.Properties;

/**
 * Azure Blob Storage-based distributed lock client using ETag checks.
 * Supports both WASB(S) and ABFS(S) URI schemes for Azure Data Lake Storage Gen2.
 *
 * <p>Authentication is handled automatically via DefaultAzureCredential which supports:
 * <ul>
 *   <li>Managed Identity (Azure VMs, App Service, Container Instances, AKS)</li>
 *   <li>Workload Identity (Kubernetes service accounts)</li>
 *   <li>Environment variables (AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_CLIENT_SECRET)</li>
 *   <li>Azure CLI credentials (for local development)</li>
 *   <li>VS Code, IntelliJ, and Azure PowerShell credentials</li>
 * </ul>
 *
 * <p>No configuration is required when running on Azure infrastructure.
 * See RFC: <a href="https://github.com/apache/hudi/blob/master/rfc/rfc-91/rfc-91.md">RFC-91</a>
 */
@ThreadSafe
public class AzureStorageLockClient implements StorageLockClient {
  private static final Logger LOG = LoggerFactory.getLogger(AzureStorageLockClient.class);
  private static final int PRECONDITION_FAILURE_ERROR_CODE = 412;
  private static final int NOT_FOUND_ERROR_CODE = 404;
  private static final int CONFLICT_ERROR_CODE = 409;
  private static final int RATE_LIMIT_ERROR_CODE = 429;
  private static final int INTERNAL_SERVER_ERROR_CODE_MIN = 500;

  private final Logger logger;
  private final BlobClient blobClient;
  private final String containerName;
  private final String blobPath;
  private final String ownerId;

  /**
   * Constructor that is used by reflection to instantiate an Azure-based storage locking client.
   *
   * @param ownerId     The owner id.
   * @param lockFileUri The full table base path where the lock will be written.
   * @param props       The properties for the lock config, can be used to customize client.
   */
  public AzureStorageLockClient(String ownerId, String lockFileUri, Properties props) {
    this(ownerId, lockFileUri, props, createDefaultBlobClient(), LOG);
  }

  @VisibleForTesting
  AzureStorageLockClient(String ownerId, String lockFileUri, Properties props,
                         Functions.Function2<String, Properties, BlobClient> blobClientSupplier,
                         Logger logger) {
    AzureStorageUriComponents uriComponents = AzureStorageUtils.parseAzureUri(lockFileUri);
    this.containerName = uriComponents.containerName;
    this.blobPath = uriComponents.blobPath;

    this.blobClient = blobClientSupplier.apply(lockFileUri, props);
    this.ownerId = ownerId;
    this.logger = logger;
  }

  @Override
  public Pair<LockGetResult, Option<StorageLockFile>> readCurrentLockFile() {
    try {
      BlobDownloadContentResponse response = blobClient.downloadContentWithResponse(
          null,
          null,
          null,
          Context.NONE
      );

      // Get ETag from response headers
      String eTag = response.getHeaders().getValue("ETag");
      if (eTag != null) {
        // Azure returns ETags wrapped in quotes, remove them
        eTag = eTag.replaceAll("^\"|\"$", "");
      }

      byte[] content = response.getValue().toBytes();
      ByteArrayInputStream inputStream = new ByteArrayInputStream(content);

      return Pair.of(LockGetResult.SUCCESS,
          Option.of(StorageLockFile.createFromStream(inputStream, eTag)));
    } catch (BlobStorageException e) {
      int status = e.getStatusCode();
      LockGetResult result = LockGetResult.UNKNOWN_ERROR;

      if (status == NOT_FOUND_ERROR_CODE || e.getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND) {
        logger.info("OwnerId: {}, Blob not found: {}", ownerId, blobPath);
        result = LockGetResult.NOT_EXISTS;
      } else if (status == CONFLICT_ERROR_CODE) {
        logger.info("OwnerId: {}, Conflicting operation has occurred: {}", ownerId, blobPath);
      } else if (status == RATE_LIMIT_ERROR_CODE) {
        logger.warn("OwnerId: {}, Rate limit exceeded: {}", ownerId, blobPath);
      } else if (status >= INTERNAL_SERVER_ERROR_CODE_MIN) {
        logger.warn("OwnerId: {}, Azure internal server error: {}", ownerId, blobPath, e);
      } else {
        throw e;
      }
      return Pair.of(result, Option.empty());
    }
  }

  @Override
  public Pair<LockUpsertResult, Option<StorageLockFile>> tryUpsertLockFile(
      StorageLockData newLockData, Option<StorageLockFile> previousLockFile) {
    boolean isLockRenewal = previousLockFile.isPresent();
    String currentEtag = isLockRenewal ? previousLockFile.get().getVersionId() : null;

    try {
      StorageLockFile updated = createOrUpdateLockFileInternal(newLockData, currentEtag);
      return Pair.of(LockUpsertResult.SUCCESS, Option.of(updated));
    } catch (BlobStorageException e) {
      LockUpsertResult result = handleUpsertBlobStorageException(e);
      return Pair.of(result, Option.empty());
    } catch (HttpResponseException e) {
      logger.error("OwnerId: {}, Unexpected Azure SDK error while writing lock file: {}",
          ownerId, blobPath, e);
      if (!isLockRenewal) {
        // We should always throw errors early when we are creating the lock file.
        // This is likely indicative of a larger issue that should bubble up sooner.
        throw e;
      }
      return Pair.of(LockUpsertResult.UNKNOWN_ERROR, Option.empty());
    }
  }

  /**
   * Internal helper to create or update the lock file with optional ETag precondition.
   */
  private StorageLockFile createOrUpdateLockFileInternal(StorageLockData lockData, String expectedEtag) {
    byte[] bytes = StorageLockFile.toByteArray(lockData);
    BinaryData binaryData = BinaryData.fromBytes(bytes);
    BlobRequestConditions requestConditions = new BlobRequestConditions();

    // ETag-based constraints:
    // - If expectedEtag is not null:
    //    We assume that the blob already exists with the ETag "expectedEtag".
    //    The update operation will include an ifMatch(expectedEtag) condition, meaning the update will only
    //    succeed if the current blob's ETag exactly matches expectedEtag.
    //    If the actual ETag of the blob differs from expectedEtag, the update attempt will fail.
    // - If expectedEtag is null:
    //    We assume that the blob does not currently exist.
    //    The operation will use ifNoneMatch("*"), which instructs Azure to create the blob only if it doesn't already exist.
    //    If a blob with the same name is present (i.e., there is an existing ETag), the creation attempt will fail.
    if (expectedEtag == null) {
      requestConditions.setIfNoneMatch("*");
    } else {
      requestConditions.setIfMatch(expectedEtag);
    }

    BlobParallelUploadOptions options = new BlobParallelUploadOptions(binaryData)
        .setRequestConditions(requestConditions);

    Response<BlockBlobItem> response = blobClient.uploadWithResponse(options, null, Context.NONE);
    String newEtag = response.getValue().getETag();

    return new StorageLockFile(lockData, newEtag);
  }

  private LockUpsertResult handleUpsertBlobStorageException(BlobStorageException e) {
    int status = e.getStatusCode();

    // Both conditions check for HTTP 412 Precondition Failed
    // status == 412: Numeric HTTP status code
    // errorCode == CONDITION_NOT_MET: Azure's semantic error code for the same condition
    // We check both for robustness across different SDK versions and error scenarios
    if (status == PRECONDITION_FAILURE_ERROR_CODE
        || e.getErrorCode() == BlobErrorCode.CONDITION_NOT_MET) {
      logger.info("OwnerId: {}, Lock file modified by another process: {}", ownerId, blobPath);
      return LockUpsertResult.ACQUIRED_BY_OTHERS;
    } else if (status == CONFLICT_ERROR_CODE) {
      logger.info("OwnerId: {}, Retriable conditional request conflict error: {}", ownerId, blobPath);
    } else if (status == RATE_LIMIT_ERROR_CODE) {
      logger.warn("OwnerId: {}, Rate limit exceeded for: {}", ownerId, blobPath);
    } else if (status >= INTERNAL_SERVER_ERROR_CODE_MIN) {
      logger.warn("OwnerId: {}, Internal server error for: {}", ownerId, blobPath, e);
    } else {
      logger.warn("OwnerId: {}, Error writing lock file: {}", ownerId, blobPath, e);
    }

    return LockUpsertResult.UNKNOWN_ERROR;
  }

  private static Functions.Function2<String, Properties, BlobClient> createDefaultBlobClient() {
    return (lockFileUri, props) -> {
      AzureStorageUriComponents uriComponents = AzureStorageUtils.parseAzureUri(lockFileUri);

      URI uri = URI.create(lockFileUri);
      String scheme = uri.getScheme();

      // Determine the appropriate endpoint based on scheme
      // ABFS uses DFS endpoint for ADLS Gen2, WASB uses Blob endpoint
      // Note: Both endpoints are compatible with BlobServiceClient in Azure SDK 12.14.0+
      String endpoint;
      if (scheme.startsWith("abfs")) {
        endpoint = String.format("https://%s.dfs.core.windows.net", uriComponents.accountName);
      } else {
        endpoint = String.format("https://%s.blob.core.windows.net", uriComponents.accountName);
      }

      // Configure timeout options based on lock validity timeout
      // Set all request timeouts to be 1/5 of the default validity.
      // Each call to acquire a lock requires 2 requests.
      // Each renewal requires 1 request.
      long validityTimeoutSecs = ((Number) props.getOrDefault(
          StorageBasedLockConfig.VALIDITY_TIMEOUT_SECONDS.key(),
          StorageBasedLockConfig.VALIDITY_TIMEOUT_SECONDS.defaultValue()
      )).longValue();
      long azureCallTimeoutSecs = validityTimeoutSecs / 5;

      // Disable retries for lock operations - retries can cause deadlocks and unpredictable timing
      // Lock operations should fail fast to allow higher-level retry logic to handle conflicts
      RequestRetryOptions retryOptions = new RequestRetryOptions(
          RetryPolicyType.EXPONENTIAL,
          1,                                      // maxTries - 1 means no retries (try once only)
          (int) azureCallTimeoutSecs,            // tryTimeout - max time per HTTP request attempt (seconds)
          null,                                   // retryDelay - not used with maxTries=1
          null,                                   // maxRetryDelay - not used with maxTries=1
          null                                    // secondaryHost - not using secondary regions
      );

      // Configure HTTP client timeouts
      HttpClientOptions clientOptions = new HttpClientOptions()
          .responseTimeout(Duration.ofSeconds(azureCallTimeoutSecs))
          .readTimeout(Duration.ofSeconds(azureCallTimeoutSecs));

      // Use DefaultAzureCredential for automatic authentication
      // This supports: Managed Identity, Workload Identity, Environment Variables,
      // Azure CLI, VS Code, IntelliJ, and Azure PowerShell credentials
      BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
          .endpoint(endpoint)
          .credential(new DefaultAzureCredentialBuilder().build())
          .retryOptions(retryOptions)
          .clientOptions(clientOptions)
          .buildClient();

      BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(uriComponents.containerName);
      return containerClient.getBlobClient(uriComponents.blobPath);
    };
  }

  @Override
  public Option<String> readObject(String filePath, boolean checkExistsFirst) {
    try {
      AzureStorageUriComponents uriComponents = AzureStorageUtils.parseAzureUri(filePath);

      // Validate that the container name matches the lock client's container
      ValidationUtils.checkArgument(
          uriComponents.containerName.equals(this.containerName),
          String.format("Container name mismatch: expected '%s' but got '%s' in path '%s'",
              this.containerName, uriComponents.containerName, filePath));

      BlobClient client = blobClient.getContainerClient()
          .getBlobClient(uriComponents.blobPath);

      if (checkExistsFirst) {
        // Note: client.exists() returns false for missing blobs without throwing an exception
        // No need to catch BlobStorageException with NOT_FOUND here
        if (!client.exists()) {
          logger.debug("JSON config file not found: {}", filePath);
          return Option.empty();
        }
      }

      String content = client.downloadContent().toString();
      return Option.of(content);
    } catch (BlobStorageException e) {
      // Handle NOT_FOUND errors when checkExistsFirst is false
      if (e.getStatusCode() == NOT_FOUND_ERROR_CODE
          || e.getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND) {
        logger.debug("JSON config file not found: {}", filePath);
      } else {
        logger.error("Error reading JSON config file: {}", filePath, e);
      }
      return Option.empty();
    } catch (Exception e) {
      logger.error("Error reading JSON config file: {}", filePath, e);
      return Option.empty();
    }
  }

  @Override
  public boolean writeObject(String filePath, String content) {
    try {
      AzureStorageUriComponents uriComponents = AzureStorageUtils.parseAzureUri(filePath);

      // Validate that the container name matches the lock client's container
      ValidationUtils.checkArgument(
          uriComponents.containerName.equals(this.containerName),
          String.format("Container name mismatch: expected '%s' but got '%s' in path '%s'",
              this.containerName, uriComponents.containerName, filePath));

      BlobClient client = blobClient.getContainerClient()
          .getBlobClient(uriComponents.blobPath);

      client.upload(BinaryData.fromString(content), true);

      logger.debug("Successfully wrote object to: {}", filePath);
      return true;
    } catch (Exception e) {
      logger.error("Error writing object to: {}", filePath, e);
      return false;
    }
  }

  @Override
  public void close() {
    // BlobClient doesn't require explicit cleanup
  }
}
