<!--
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
-->

# `hudi-io` Module

This module contains classes that are I/O related, including common abstraction and APIs, readers and writers, etc.

## HFile Reader

We implement our own HFile reader (`org.apache.hudi.io.hfile.HFileReaderImpl`) that functionally works on reading HBase
HFiles in the Hudi metadata tables, based on the format described below.

## HFile Format

Refer to [HFile Format](hfile_format.md) documentation.