/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import type { ErrorLogInfo } from '/#/store';

import { defineStore } from 'pinia';
import { store } from '/@/store';

import { formatToDateTime } from '/@/utils/dateUtil';
import projectSetting from '/@/settings/projectSetting';

import { ErrorTypeEnum } from '/@/enums/exceptionEnum';

export interface ErrorLogState {
  errorLogInfoList: Nullable<ErrorLogInfo[]>;
  errorLogListCount: number;
}

export const useErrorLogStore = defineStore({
  id: 'app-error-log',
  state: (): ErrorLogState => ({
    errorLogInfoList: null,
    errorLogListCount: 0,
  }),
  getters: {
    getErrorLogInfoList(): ErrorLogInfo[] {
      return this.errorLogInfoList || [];
    },
    getErrorLogListCount(): number {
      return this.errorLogListCount;
    },
  },
  actions: {
    addErrorLogInfo(info: ErrorLogInfo) {
      const item = {
        ...info,
        time: formatToDateTime(new Date()),
      };
      this.errorLogInfoList = [item, ...(this.errorLogInfoList || [])];
      this.errorLogListCount += 1;
    },

    setErrorLogListCount(count: number): void {
      this.errorLogListCount = count;
    },

    /**
     * Triggered after ajax request error
     * @param error
     * @returns
     */
    addAjaxErrorInfo(error) {
      const { useErrorHandle } = projectSetting;
      if (!useErrorHandle) {
        return;
      }
      const errInfo: Partial<ErrorLogInfo> = {
        message: error.message,
        type: ErrorTypeEnum.AJAX,
      };
      if (error.response) {
        const {
          config: { url = '', data: params = '', method = 'get', headers = {} } = {},
          data = {},
        } = error.response;
        errInfo.url = url;
        errInfo.name = 'Ajax Error!';
        errInfo.file = '-';
        errInfo.stack = JSON.stringify(data);
        errInfo.detail = JSON.stringify({ params, method, headers });
      }
      this.addErrorLogInfo(errInfo as ErrorLogInfo);
    },
  },
});

// Need to be used outside the setup
export function useErrorLogStoreWithOut() {
  return useErrorLogStore(store);
}
