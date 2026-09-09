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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link DynamoTableUtils} against a mocked DynamoDB client. The polling helpers are always
 * given an explicit, short timeout so the tests stay fast and deterministic.
 */
@ExtendWith(MockitoExtension.class)
class TestDynamoTableUtils {

  private static final String TABLE_NAME = "lock_table";
  private static final int TIMEOUT_MS = 300;
  private static final int INTERVAL_MS = 50;

  @Mock
  private DynamoDbClient dynamoDb;

  @Test
  void testWaitUntilExists_returnsOnTheFirstDescription() throws Exception {
    when(dynamoDb.describeTable(any(DescribeTableRequest.class))).thenReturn(describeResponse(TableStatus.CREATING));

    DynamoTableUtils.waitUntilExists(dynamoDb, TABLE_NAME, TIMEOUT_MS, INTERVAL_MS);

    ArgumentCaptor<DescribeTableRequest> captor = ArgumentCaptor.forClass(DescribeTableRequest.class);
    verify(dynamoDb, times(1)).describeTable(captor.capture());
    assertEquals(TABLE_NAME, captor.getValue().tableName(),
        "any table status is enough to prove the table exists");
  }

  @Test
  void testWaitUntilExists_pollsUntilTheTableShowsUp() throws Exception {
    when(dynamoDb.describeTable(any(DescribeTableRequest.class)))
        .thenThrow(ResourceNotFoundException.builder().message("not there yet").build())
        .thenReturn(describeResponse(TableStatus.ACTIVE));

    DynamoTableUtils.waitUntilExists(dynamoDb, TABLE_NAME, TIMEOUT_MS, INTERVAL_MS);

    verify(dynamoDb, times(2)).describeTable(any(DescribeTableRequest.class));
  }

  @Test
  void testWaitUntilExists_throwsWhenTheTableNeverShowsUp() {
    when(dynamoDb.describeTable(any(DescribeTableRequest.class)))
        .thenThrow(ResourceNotFoundException.builder().message("not there yet").build());

    SdkClientException ex = assertThrows(SdkClientException.class,
        () -> DynamoTableUtils.waitUntilExists(dynamoDb, TABLE_NAME, TIMEOUT_MS, INTERVAL_MS));

    assertTrue(ex.getMessage().contains(TABLE_NAME + " never returned a result"));
    verify(dynamoDb, atLeast(2)).describeTable(any(DescribeTableRequest.class));
  }

  @Test
  void testWaitUntilActive_returnsWhenTheTableIsActive() throws Exception {
    when(dynamoDb.describeTable(any(DescribeTableRequest.class)))
        .thenReturn(describeResponse(TableStatus.CREATING))
        .thenReturn(describeResponse(TableStatus.ACTIVE));

    DynamoTableUtils.waitUntilActive(dynamoDb, TABLE_NAME, TIMEOUT_MS, INTERVAL_MS);

    verify(dynamoDb, times(2)).describeTable(any(DescribeTableRequest.class));
  }

  @Test
  void testWaitUntilActive_throwsWhenTheTableStaysInAnotherState() {
    when(dynamoDb.describeTable(any(DescribeTableRequest.class))).thenReturn(describeResponse(TableStatus.CREATING));

    DynamoTableUtils.TableNeverTransitionedToStateException ex =
        assertThrows(DynamoTableUtils.TableNeverTransitionedToStateException.class,
            () -> DynamoTableUtils.waitUntilActive(dynamoDb, TABLE_NAME, TIMEOUT_MS, INTERVAL_MS));

    assertTrue(ex.getMessage().contains(TABLE_NAME + " never transitioned to desired state of ACTIVE"));
  }

  @Test
  void testWaitUntilActive_throwsWhenTheTableNeverAppears() {
    when(dynamoDb.describeTable(any(DescribeTableRequest.class)))
        .thenThrow(ResourceNotFoundException.builder().message("not there yet").build());

    assertThrows(DynamoTableUtils.TableNeverTransitionedToStateException.class,
        () -> DynamoTableUtils.waitUntilActive(dynamoDb, TABLE_NAME, TIMEOUT_MS, INTERVAL_MS),
        "a table that never gets described is reported the same way as one stuck in another state");
  }

  @Test
  void testDefaultTimeoutOverloadsReturnAsSoonAsTheTableIsReady() throws Exception {
    when(dynamoDb.describeTable(any(DescribeTableRequest.class))).thenReturn(describeResponse(TableStatus.ACTIVE));

    // both overloads poll before sleeping, so an already-ready table returns without waiting
    DynamoTableUtils.waitUntilExists(dynamoDb, TABLE_NAME);
    DynamoTableUtils.waitUntilActive(dynamoDb, TABLE_NAME);

    verify(dynamoDb, times(2)).describeTable(any(DescribeTableRequest.class));
  }

  @ParameterizedTest
  @CsvSource({"-1, 10", "100, 0", "100, 100", "100, 200"})
  void testWaitUntilExists_rejectsInvalidTimeoutAndInterval(int timeout, int interval) {
    assertThrows(IllegalArgumentException.class,
        () -> DynamoTableUtils.waitUntilExists(dynamoDb, TABLE_NAME, timeout, interval));
  }

  @Test
  void testCreateTableIfNotExists_reportsWhetherTheTableWasCreated() {
    CreateTableRequest request = CreateTableRequest.builder().tableName(TABLE_NAME).build();
    when(dynamoDb.createTable(request))
        .thenReturn(CreateTableResponse.builder().build())
        .thenThrow(ResourceInUseException.builder().message("already there").build());

    assertTrue(DynamoTableUtils.createTableIfNotExists(dynamoDb, request));
    assertFalse(DynamoTableUtils.createTableIfNotExists(dynamoDb, request),
        "an already existing table is not an error");
  }

  @Test
  void testDeleteTableIfExists_reportsWhetherTheTableWasDeleted() {
    DeleteTableRequest request = DeleteTableRequest.builder().tableName(TABLE_NAME).build();
    when(dynamoDb.deleteTable(request))
        .thenReturn(DeleteTableResponse.builder().build())
        .thenThrow(ResourceNotFoundException.builder().message("gone").build());

    assertTrue(DynamoTableUtils.deleteTableIfExists(dynamoDb, request));
    assertFalse(DynamoTableUtils.deleteTableIfExists(dynamoDb, request),
        "a missing table is not an error");
  }

  private static DescribeTableResponse describeResponse(TableStatus status) {
    return DescribeTableResponse.builder()
        .table(TableDescription.builder().tableName(TABLE_NAME).tableStatus(status).build())
        .build();
  }
}
