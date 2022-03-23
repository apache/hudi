/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.adapter;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.YieldingOperatorFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Adapter clazz for {@link AbstractStreamOperatorFactory}.
 */
public abstract class AbstractStreamOperatorFactoryAdapter<O>
    extends AbstractStreamOperatorFactory<O> implements YieldingOperatorFactory<O> {
  private transient MailboxExecutor mailboxExecutor;

  @Override
  public void setMailboxExecutor(MailboxExecutor mailboxExecutor) {
    this.mailboxExecutor = mailboxExecutor;
  }

  public MailboxExecutorAdapter getMailboxExecutorAdapter() {
    return new MailboxExecutorAdapter(getMailboxExecutor());
  }

  /**
   * Provides the mailbox executor iff this factory implements {@link YieldingOperatorFactory}.
   */
  protected MailboxExecutor getMailboxExecutor() {
    return checkNotNull(
        mailboxExecutor, "Factory does not implement %s", YieldingOperatorFactory.class);
  }
}
