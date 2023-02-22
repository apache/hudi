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

package org.apache.hudi.metaserver.util;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.net.ServerSocket;

/**
 * A wrapper class of thrift server socket.
 */
public class TServerSocketWrapper extends TServerSocket {

  private static final Logger LOG = LogManager.getLogger(TServerSocketWrapper.class);

  public TServerSocketWrapper(ServerSocket serverSocket) throws TTransportException {
    super(serverSocket);
  }

  public TServerSocketWrapper(int port) throws TTransportException {
    super(port);
  }

  @Override
  protected TSocket acceptImpl() throws TTransportException {
    TSocket socket = super.acceptImpl();
    LOG.info("received connection from " + socket.getSocket().getInetAddress().getHostAddress()
        + ":" + socket.getSocket().getPort());
    return socket;
  }
}
