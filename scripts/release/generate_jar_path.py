#!/usr/bin/python

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

import fileinput
import os

for line in fileinput.input():
  line = line.rstrip()
  cols = line.split(":")
  classifier = ""
  scope = ""
  pkg = ""
  type = ""
  version = ""
  if len(cols) == 4:
   (scope,pkg,type,version) = cols
  else:
   (scope,pkg,type,classifier,version) = cols
   classifier = "-{}".format(classifier)
  path = "{5},{6}/.m2/repository/{0}/{1}/{2}/{1}-{2}{3}.{4}".format(scope.replace(".","/"), pkg, version,classifier,type, line, os.environ['HOME'])
  print path
   
