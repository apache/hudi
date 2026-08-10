#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -e

path=`echo $1 | cut -f 2 -d ","`;
mvncoord=`echo $1 | cut -f 1 -d ","`;
scope=`echo ${mvncoord} | cut -f 1 -d ":"`;
mkdir tmp > /dev/null
pushd tmp > /dev/null
jar -xf $path

pkg=`echo $path | rev | cut -d "/" -f 1 | rev | sed -e 's/\.jar//'`
echo "The binary distribution of this org.apache.hudi package bundles binary of ${scope}:$pkg with the following NOTICE"
echo "============================================================================================================"
find . -iname '*NOTICE*' | xargs cat 
echo "============================================================================================================"
echo ""
echo ""
popd > /dev/null
rm -rf tmp
