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

package org.apache.hudi.client.transaction;


/**
 * This strategy is used for direct marker writers, trying to do early conflict detection with Transaction.
 * It will use fileSystem api like list and exist directly to check if there is any marker file conflict.
 */
public class DirectMarkerWithTransactionConflictResolutionStrategy extends SimpleDirectMarkerConflictResolutionStrategy
    implements TransactionConflictResolutionStrategy {
}
