#!/usr/bin/env bash
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

set -euo pipefail
# Named volumes start root-owned. Only this bootstrap runs as root; HMS and
# Derby initialization run as the image's hive user, as in local-dev.
if [[ "$(id -u)" == 0 ]]; then
  chown hive:hive /opt/hive/data
  exec su -s /bin/bash hive -c 'exec bash /lakehouse/start-metastore.sh'
fi
export HADOOP_CLASSPATH="$(echo "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-aws-*.jar):$(echo "$HADOOP_HOME"/share/hadoop/tools/lib/aws-java-sdk-bundle-*.jar)${HADOOP_CLASSPATH:+:$HADOOP_CLASSPATH}"
# The marker is only a shortcut: initialization may have completed before a
# crash, or an existing volume may predate the marker.
if [[ ! -f /opt/hive/data/.schema-initialized ]]; then
  if [[ -e /opt/hive/data/metastore_db || -L /opt/hive/data/metastore_db ]]; then
    # Do not let the configured create=true URL create a database while checking
    # an existing (possibly incomplete) database directory.
    if ! "$HIVE_HOME/bin/schematool" -dbType derby -validate \
        -url 'jdbc:derby:;databaseName=/opt/hive/data/metastore_db'; then
      echo 'Existing Derby metastore at /opt/hive/data/metastore_db failed schema validation. Refusing to initialize over existing data.' >&2
      echo 'Back up the metastore volume, inspect the schematool errors above, and repair or restore the database before restarting. No data has been deleted.' >&2
      exit 1
    fi
  else
    if ! "$HIVE_HOME/bin/schematool" -dbType derby -initSchema; then
      echo 'Derby metastore initialization failed. Inspect the schematool errors above; back up and repair or restore any partial database at /opt/hive/data/metastore_db before restarting.' >&2
      exit 1
    fi
  fi
  touch /opt/hive/data/.schema-initialized
fi
export IS_RESUME=true
exec /entrypoint.sh
