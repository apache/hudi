# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
FROM apache/superset:6.1.0
USER root
COPY . /tmp/hudi-spark-connect-superset
RUN uv pip install --python /app/.venv/bin/python /tmp/hudi-spark-connect-superset 'dbc==0.3.1' \
    && /app/.venv/bin/dbc install --level system 'spark=0.2.1' \
    && mkdir -p /app/superset_home/.config \
    && chown -R superset:superset /app/superset_home/.config \
    && rm -rf /tmp/hudi-spark-connect-superset
USER superset
