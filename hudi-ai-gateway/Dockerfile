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

# The hudi-ai-gateway server image. Build context: this directory.
FROM python:3.12-slim

RUN groupadd --gid 1000 gateway && \
    useradd --uid 1000 --gid gateway --create-home gateway

WORKDIR /opt/hudi-ai-gateway
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

USER gateway
EXPOSE 8000
ENTRYPOINT ["hudi-ai-gateway"]
CMD ["serve"]
