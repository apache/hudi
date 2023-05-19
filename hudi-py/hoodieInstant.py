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

import re
import json
import fsspec

REQUESTED_STATE = "requested"
INFLIGHT_STATE = "inflight"
COMPLETED_STATE = "completed"
UNKNOWN_STATE = "unknown"

COMMIT_ACTION = "commit"
DELTA_COMMIT_ACTION = "deltacommit"
CLEAN_ACTION = "clean"
ROLLBACK_ACTION = "rollback"
SAVEPOINT_ACTION = "savepoint"
REPLACE_COMMIT_ACTION = "replacecommit"
COMPACTION_ACTION = "compaction"
LOG_COMPACTION_ACTION = "logcompaction"
RESTORE_ACTION = "restore"
INDEXING_ACTION = "indexing"
SCHEMA_COMMIT_ACTION = "schemacommit"


VALID_ACTIONS_IN_TIMELINE = [COMMIT_ACTION, DELTA_COMMIT_ACTION, CLEAN_ACTION, 
                             SAVEPOINT_ACTION, RESTORE_ACTION, ROLLBACK_ACTION,
                             COMPACTION_ACTION, REPLACE_COMMIT_ACTION, INDEXING_ACTION]


INVALID_INSTANT_TS = "0"
INIT_INSTANT_TS = "00000000000000"
METADATA_BOOTSTRAP_INSTANT_TS = "00000000000001"
FULL_BOOTSTRAP_INSTANT_TS = "00000000000002"

REQUESTED_EXTENSION = ".requested"
INFLIGHT_EXTENSION = ".inflight"

COMMIT_EXTENSION = "." + COMMIT_ACTION
DELTA_COMMIT_EXTENSION = "." + DELTA_COMMIT_ACTION
CLEAN_EXTENSION = "." + CLEAN_ACTION
ROLLBACK_EXTENSION = "." + ROLLBACK_ACTION
SAVEPOINT_EXTENSION = "." + SAVEPOINT_ACTION
INFLIGHT_COMMIT_EXTENSION = INFLIGHT_EXTENSION
REQUESTED_COMMIT_EXTENSION = "." + COMMIT_ACTION + REQUESTED_EXTENSION
REQUESTED_DELTA_COMMIT_EXTENSION = "." + DELTA_COMMIT_ACTION + REQUESTED_EXTENSION
INFLIGHT_DELTA_COMMIT_EXTENSION = "." + DELTA_COMMIT_ACTION + INFLIGHT_EXTENSION
INFLIGHT_CLEAN_EXTENSION = "." + CLEAN_ACTION + INFLIGHT_EXTENSION
REQUESTED_CLEAN_EXTENSION = "." + CLEAN_ACTION + REQUESTED_EXTENSION
INFLIGHT_ROLLBACK_EXTENSION = "." + ROLLBACK_ACTION + INFLIGHT_EXTENSION
REQUESTED_ROLLBACK_EXTENSION = "." + ROLLBACK_ACTION + REQUESTED_EXTENSION
INFLIGHT_SAVEPOINT_EXTENSION = "." + SAVEPOINT_ACTION + INFLIGHT_EXTENSION
REQUESTED_COMPACTION_SUFFIX = COMPACTION_ACTION + REQUESTED_EXTENSION
REQUESTED_COMPACTION_EXTENSION = "." + REQUESTED_COMPACTION_SUFFIX
INFLIGHT_COMPACTION_EXTENSION = "." + COMPACTION_ACTION + INFLIGHT_EXTENSION
REQUESTED_RESTORE_EXTENSION = "." + RESTORE_ACTION + REQUESTED_EXTENSION
INFLIGHT_RESTORE_EXTENSION = "." + RESTORE_ACTION + INFLIGHT_EXTENSION
RESTORE_EXTENSION = "." + RESTORE_ACTION
INFLIGHT_REPLACE_COMMIT_EXTENSION = "." + REPLACE_COMMIT_ACTION + INFLIGHT_EXTENSION
REQUESTED_REPLACE_COMMIT_EXTENSION = "." + REPLACE_COMMIT_ACTION + REQUESTED_EXTENSION
REPLACE_COMMIT_EXTENSION = "." + REPLACE_COMMIT_ACTION
INFLIGHT_INDEX_COMMIT_EXTENSION = "." + INDEXING_ACTION + INFLIGHT_EXTENSION
REQUESTED_INDEX_COMMIT_EXTENSION = "." + INDEXING_ACTION + REQUESTED_EXTENSION
INDEX_COMMIT_EXTENSION = "." + INDEXING_ACTION
SAVE_SCHEMA_ACTION_EXTENSION = "." + SCHEMA_COMMIT_ACTION
INFLIGHT_SAVE_SCHEMA_ACTION_EXTENSION = "." + SCHEMA_COMMIT_ACTION + INFLIGHT_EXTENSION
REQUESTED_SAVE_SCHEMA_ACTION_EXTENSION = "." + SCHEMA_COMMIT_ACTION + REQUESTED_EXTENSION
REQUESTED_LOG_COMPACTION_SUFFIX = LOG_COMPACTION_ACTION + REQUESTED_EXTENSION
REQUESTED_LOG_COMPACTION_EXTENSION = "." + REQUESTED_LOG_COMPACTION_SUFFIX
INFLIGHT_LOG_COMPACTION_EXTENSION = "." + LOG_COMPACTION_ACTION + INFLIGHT_EXTENSION

VALID_EXTENSIONS_IN_ACTIVE_TIMELINE = {COMMIT_EXTENSION, INFLIGHT_COMMIT_EXTENSION, REQUESTED_COMMIT_EXTENSION,
      DELTA_COMMIT_EXTENSION, INFLIGHT_DELTA_COMMIT_EXTENSION, REQUESTED_DELTA_COMMIT_EXTENSION,
      SAVEPOINT_EXTENSION, INFLIGHT_SAVEPOINT_EXTENSION,
      CLEAN_EXTENSION, REQUESTED_CLEAN_EXTENSION, INFLIGHT_CLEAN_EXTENSION,
      INFLIGHT_COMPACTION_EXTENSION, REQUESTED_COMPACTION_EXTENSION,
      REQUESTED_RESTORE_EXTENSION, INFLIGHT_RESTORE_EXTENSION, RESTORE_EXTENSION,
      INFLIGHT_LOG_COMPACTION_EXTENSION, REQUESTED_LOG_COMPACTION_EXTENSION,
      ROLLBACK_EXTENSION, REQUESTED_ROLLBACK_EXTENSION, INFLIGHT_ROLLBACK_EXTENSION,
      REQUESTED_REPLACE_COMMIT_EXTENSION, INFLIGHT_REPLACE_COMMIT_EXTENSION, REPLACE_COMMIT_EXTENSION,
      REQUESTED_INDEX_COMMIT_EXTENSION, INFLIGHT_INDEX_COMMIT_EXTENSION, INDEX_COMMIT_EXTENSION,
      REQUESTED_SAVE_SCHEMA_ACTION_EXTENSION, INFLIGHT_SAVE_SCHEMA_ACTION_EXTENSION, SAVE_SCHEMA_ACTION_EXTENSION}

extensionToAction = {COMMIT_EXTENSION: COMMIT_ACTION, 
                     REQUESTED_COMMIT_EXTENSION: COMMIT_ACTION,
                     INFLIGHT_COMMIT_EXTENSION: COMMIT_ACTION,
                     REPLACE_COMMIT_EXTENSION: REPLACE_COMMIT_ACTION,
                     REQUESTED_REPLACE_COMMIT_EXTENSION: REPLACE_COMMIT_ACTION,
                     INFLIGHT_REPLACE_COMMIT_EXTENSION: REPLACE_COMMIT_ACTION}

extensionToState = {COMMIT_EXTENSION: COMPLETED_STATE, 
                     REQUESTED_COMMIT_EXTENSION: REQUESTED_STATE,
                     INFLIGHT_COMMIT_EXTENSION: INFLIGHT_STATE,
                     REPLACE_COMMIT_EXTENSION: COMPLETED_STATE,
                     REQUESTED_REPLACE_COMMIT_EXTENSION: REQUESTED_STATE,
                     INFLIGHT_REPLACE_COMMIT_EXTENSION: INFLIGHT_STATE}

stateToValue = {REQUESTED_STATE: "0",
                INFLIGHT_STATE: "1",
                COMPLETED_STATE: "2",
                UNKNOWN_STATE: "3"}

class HoodieInstant:

    def __init__(self, file: str, fs: fsspec.AbstractFileSystem):
        self.file = file
        stripName = file.rsplit("/")[-1]
        matches = re.findall("(\d+)(\.\w+)(\.\D+)?", stripName)
        self.valid = len(matches) > 0
        if not self.valid:
            return
        self.timestamp = matches[0][0]
        self.extension = stripName[len(matches[0][0]):]
        self.action = HoodieInstant._parseAction(self.extension)
        self.state = HoodieInstant._parseState(self.extension)
        self.invalidFiles : dict[str, set[str]] = self._getInvalidFiles(fs)

    def _getInvalidFiles(self, fs: fsspec.AbstractFileSystem) -> dict[str,list[str]]:
        invalidFiles: dict[str, set[str]] = {}
        if self.action == REPLACE_COMMIT_ACTION and self.state == COMPLETED_STATE:
            clusterContent = json.loads(fs.cat_file(self.file))
            for partition in clusterContent["partitionToReplaceFileIds"]:
                invalidFiles[partition] = set()
                for fileId in clusterContent["partitionToReplaceFileIds"][partition]:
                    invalidFiles[partition].add(fileId)
        return invalidFiles

        
    def _parseAction(extension: str): 
        if extension in extensionToAction:
            return extensionToAction[extension]
        return "unknown"
    
    def _parseState(extension: str):
        if extension in extensionToState:
            return extensionToState[extension]
        return "unknown"
    
    def isAsOf(self, timestamp: str) -> bool:
        return self.timestamp <= timestamp
    
    def isCompleted(self) -> bool:
        return self.state == COMPLETED_STATE
    
    def isCommit(self) -> bool:
        return self.action == COMMIT_ACTION
    
    def isReplaceCommit(self) -> bool:
        return self.action == REPLACE_COMMIT_ACTION
            
    
    def sort_key(instant):
       return instant.timestamp + stateToValue[instant.state]

    def __repr__(self) -> str:
        return  self.timestamp + "__" + self.action + "__" + self.state
 
