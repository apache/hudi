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

from hoodieInstant import *


class HoodieTimeline:
    def __init__(self, instants: list[HoodieInstant]) -> None:
        self._instants = instants
        if (len(instants) > 0):
            self._start = instants[0].timestamp
            self._end = instants[-1].timestamp

    def getInstants(self):
        return self._instants
    
    def filter(self, filterfunc):
        return HoodieTimeline([instant for instant in self._instants if filterfunc(instant)])

    def getCompletedInstants(self):
        return HoodieTimeline([instant for instant in self._instants if instant.state == COMPLETED_STATE])
    
    def last(self):
        return self._instants[-1]