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

# The hudi-agent-gateway server image. Build context: this directory.
FROM python:3.12-slim

RUN groupadd --gid 1000 gateway && \
    useradd --uid 1000 --gid gateway --create-home gateway

WORKDIR /opt/hudi-agent-gateway
# Install third-party dependencies in their own layer (cached across source
# changes) by building against a stub package first, then overlay the real
# sources with a fast no-deps reinstall.
COPY pyproject.toml README.md ./
RUN mkdir -p src/hudi_agent_gateway \
    && printf '__version__ = "0.1.0"\n' > src/hudi_agent_gateway/__init__.py \
    && pip install --no-cache-dir . \
    && pip uninstall -y hudi-agent-gateway
COPY src ./src
RUN pip install --no-cache-dir --no-deps .

USER gateway
EXPOSE 8000
ENTRYPOINT ["hudi-agent-gateway"]
CMD ["serve"]
