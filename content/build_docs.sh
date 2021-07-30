#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################


echo "Please check env before building the docs. We need ruby >= 2.6 version, bundle >= 2.0.2 version. "

if [[ "`command -v ruby`" == "" ]]; then
    echo "Please install ruby first ( ruby >= 2.6 )."
    exit 1;
fi

if [[ "`command -v bundle`" == "" ]]; then
    echo "Please install bundle first ( bundle >= 2.0.2 )."
    exit 1;
fi


RUBY=${RUBY:-ruby}
GEM=${GEM:-gem}
CACHE_DIR=${CACHE_DIR:-".rubydeps"}

DIR="`pwd`"
DOCS_SRC=${DIR}
DOCS_DST=${DOCS_SRC}/web_site

set -e
cd "$(dirname ${BASH_SOURCE[0]})"

# Install Ruby dependencies
bundle install --path ${CACHE_DIR}

# Build web site
bundle exec jekyll build _config.yml --source "${DOCS_SRC}" --destination "${DOCS_DST}"

