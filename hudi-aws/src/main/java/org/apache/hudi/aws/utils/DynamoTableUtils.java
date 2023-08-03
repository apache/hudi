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

package org.apache.hudi.aws.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

/**
 * Reused code from https://github.com/aws/aws-sdk-java-v2/blob/master/services/dynamodb/src/test/java/utils/test/util/TableUtils.java
 *
 * Utility methods for working with DynamoDB tables.
 *
 * <pre class="brush: java">
 * // ... create DynamoDB table ...
 * try {
 *     waitUntilActive(dynamoDB, myTableName());
 * } catch (SdkClientException e) {
 *     // table didn't become active
 * }
 * // ... start making calls to table ...
 * </pre>
 */
public class DynamoTableUtils {

  private static final int DEFAULT_WAIT_TIMEOUT = 20 * 60 * 1000;
  private static final int DEFAULT_WAIT_INTERVAL = 10 * 1000;
  /**
   * The logging utility.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoTableUtils.class);

  /**
   * Waits up to 10 minutes for a specified DynamoDB table to resolve,
   * indicating that it exists. If the table doesn't return a result after
   * this time, a SdkClientException is thrown.
   *
   * @param dynamo
   *            The DynamoDB client to use to make requests.
   * @param tableName
   *            The name of the table being resolved.
   *
   * @throws SdkClientException
   *             If the specified table does not resolve before this method
   *             times out and stops polling.
   * @throws InterruptedException
   *             If the thread is interrupted while waiting for the table to
   *             resolve.
   */
  public static void waitUntilExists(final DynamoDbClient dynamo, final String tableName)
          throws InterruptedException {
    waitUntilExists(dynamo, tableName, DEFAULT_WAIT_TIMEOUT, DEFAULT_WAIT_INTERVAL);
  }

  /**
   * Waits up to a specified amount of time for a specified DynamoDB table to
   * resolve, indicating that it exists. If the table doesn't return a result
   * after this time, a SdkClientException is thrown.
   *
   * @param dynamo
   *            The DynamoDB client to use to make requests.
   * @param tableName
   *            The name of the table being resolved.
   * @param timeout
   *            The maximum number of milliseconds to wait.
   * @param interval
   *            The poll interval in milliseconds.
   *
   * @throws SdkClientException
   *             If the specified table does not resolve before this method
   *             times out and stops polling.
   * @throws InterruptedException
   *             If the thread is interrupted while waiting for the table to
   *             resolve.
   */
  public static void waitUntilExists(final DynamoDbClient dynamo, final String tableName, final int timeout,
                                     final int interval) throws InterruptedException {
    TableDescription table = waitForTableDescription(dynamo, tableName, null, timeout, interval);

    if (table == null) {
      throw SdkClientException.builder().message("Table " + tableName + " never returned a result").build();
    }
  }

  /**
   * Waits up to 10 minutes for a specified DynamoDB table to move into the
   * <code>ACTIVE</code> state. If the table does not exist or does not
   * transition to the <code>ACTIVE</code> state after this time, then
   * SdkClientException is thrown.
   *
   * @param dynamo
   *            The DynamoDB client to use to make requests.
   * @param tableName
   *            The name of the table whose status is being checked.
   *
   * @throws TableNeverTransitionedToStateException
   *             If the specified table does not exist or does not transition
   *             into the <code>ACTIVE</code> state before this method times
   *             out and stops polling.
   * @throws InterruptedException
   *             If the thread is interrupted while waiting for the table to
   *             transition into the <code>ACTIVE</code> state.
   */
  public static void waitUntilActive(final DynamoDbClient dynamo, final String tableName)
          throws InterruptedException, TableNeverTransitionedToStateException {
    waitUntilActive(dynamo, tableName, DEFAULT_WAIT_TIMEOUT, DEFAULT_WAIT_INTERVAL);
  }

  /**
   * Waits up to a specified amount of time for a specified DynamoDB table to
   * move into the <code>ACTIVE</code> state. If the table does not exist or
   * does not transition to the <code>ACTIVE</code> state after this time,
   * then a SdkClientException is thrown.
   *
   * @param dynamo
   *            The DynamoDB client to use to make requests.
   * @param tableName
   *            The name of the table whose status is being checked.
   * @param timeout
   *            The maximum number of milliseconds to wait.
   * @param interval
   *            The poll interval in milliseconds.
   *
   * @throws TableNeverTransitionedToStateException
   *             If the specified table does not exist or does not transition
   *             into the <code>ACTIVE</code> state before this method times
   *             out and stops polling.
   * @throws InterruptedException
   *             If the thread is interrupted while waiting for the table to
   *             transition into the <code>ACTIVE</code> state.
   */
  public static void waitUntilActive(final DynamoDbClient dynamo, final String tableName, final int timeout,
                                     final int interval) throws InterruptedException, TableNeverTransitionedToStateException {
    TableDescription table = waitForTableDescription(dynamo, tableName, TableStatus.ACTIVE, timeout, interval);

    if (table == null || !table.tableStatus().equals(TableStatus.ACTIVE)) {
      throw new TableNeverTransitionedToStateException(tableName, TableStatus.ACTIVE);
    }
  }

  /**
   * Wait for the table to reach the desired status and returns the table
   * description
   *
   * @param dynamo
   *            Dynamo client to use
   * @param tableName
   *            Table name to poll status of
   * @param desiredStatus
   *            Desired {@link TableStatus} to wait for. If null this method
   *            simply waits until DescribeTable returns something non-null
   *            (i.e. any status)
   * @param timeout
   *            Timeout in milliseconds to continue to poll for desired status
   * @param interval
   *            Time to wait in milliseconds between poll attempts
   * @return Null if DescribeTables never returns a result, otherwise the
   *         result of the last poll attempt (which may or may not have the
   *         desired state)
   * @throws {@link
   *             IllegalArgumentException} If timeout or interval is invalid
   */
  private static TableDescription waitForTableDescription(final DynamoDbClient dynamo, final String tableName,
                                                          TableStatus desiredStatus, final int timeout, final int interval)
          throws InterruptedException, IllegalArgumentException {
    if (timeout < 0) {
      throw new IllegalArgumentException("Timeout must be >= 0");
    }
    if (interval <= 0 || interval >= timeout) {
      throw new IllegalArgumentException("Interval must be > 0 and < timeout");
    }
    long startTime = System.currentTimeMillis();
    long endTime = startTime + timeout;

    TableDescription table = null;
    while (System.currentTimeMillis() < endTime) {
      try {
        table = dynamo.describeTable(DescribeTableRequest.builder().tableName(tableName).build()).table();
        if (desiredStatus == null || table.tableStatus().equals(desiredStatus)) {
          return table;

        }
      } catch (ResourceNotFoundException rnfe) {
        // ResourceNotFound means the table doesn't exist yet,
        // so ignore this error and just keep polling.
      }

      Thread.sleep(interval);
    }
    return table;
  }

  /**
   * Creates the table and ignores any errors if it already exists.
   * @param dynamo The Dynamo client to use.
   * @param createTableRequest The create table request.
   * @return True if created, false otherwise.
   */
  public static boolean createTableIfNotExists(final DynamoDbClient dynamo, final CreateTableRequest createTableRequest) {
    try {
      dynamo.createTable(createTableRequest);
      return true;
    } catch (final ResourceInUseException e) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Table " + createTableRequest.tableName() + " already exists", e);
      }
    }
    return false;
  }

  /**
   * Deletes the table and ignores any errors if it doesn't exist.
   * @param dynamo The Dynamo client to use.
   * @param deleteTableRequest The delete table request.
   * @return True if deleted, false otherwise.
   */
  public static boolean deleteTableIfExists(final DynamoDbClient dynamo, final DeleteTableRequest deleteTableRequest) {
    try {
      dynamo.deleteTable(deleteTableRequest);
      return true;
    } catch (final ResourceNotFoundException e) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Table " + deleteTableRequest.tableName() + " does not exist", e);
      }
    }
    return false;
  }

  /**
   * Thrown by {@link DynamoTableUtils} when a table never reaches a desired state
   */
  public static class TableNeverTransitionedToStateException extends SdkClientException {

    private static final long serialVersionUID = 8920567021104846647L;

    public TableNeverTransitionedToStateException(String tableName, TableStatus desiredStatus) {
      super(SdkClientException.builder()
              .message("Table " + tableName + " never transitioned to desired state of "
                      + desiredStatus.toString()));
    }

  }

}
