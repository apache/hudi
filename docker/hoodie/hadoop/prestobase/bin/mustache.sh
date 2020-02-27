#!/bin/sh

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

# `mustache.sh`, Mustache in POSIX shell.

set -e

# Load the `mustache` function and its friends.  These are assumed to be
# in the `lib` directory in the same tree as this `bin` directory.
. "$(dirname "$(dirname "$0")")/lib/mustache.sh"

# Call `mustache` to make this behave somewhat like `mustache`(1).
# Because it doesn't accept the `--compile` or `--tokens` command-line
# options and does not accept input file(s) as arguments, this program
# is called `mustache.sh`(1), not `mustache`(1).
mustache
