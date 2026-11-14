<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Complex Key Generator fixture tables

Tables written by the actual releases, copied from the `master` branch fixtures:

- `hudi-v6-table-complex-keygen.zip` - Hudi 0.14.0, table version 6 (`id:<value>` record keys)
- `hudi-v6-table-complex-keygen-bare.zip` - Hudi 0.14.1, table version 6 (bare `<value>` record keys)

Both are MOR tables with a `ComplexKeyGenerator` on the single record key field `id` and the partition fields
`partition,category`, holding `id1` to `id8` after three commits, and neither carries
`hoodie.table.complex.keygenerator.encoding`: they are the legacy tables the encoding is deduced from
(see `TestComplexKeyGenEncoding`).
