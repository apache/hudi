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
// The axios configuration can be changed according to the project, just change the file, other files can be left unchanged

import type { AxiosResponse } from 'axios';
import { clone } from 'lodash-es';
import type { RequestOptions, Result } from '/#/axios';
import type { AxiosTransform, CreateAxiosOptions } from './axiosTransform';
import { VAxios } from './Axios';
import { useGlobSetting } from '/@/hooks/setting';
import { useMessage } from '/@/hooks/web/useMessage';
import { RequestEnum, ResultEnum, ContentTypeEnum } from '/@/enums/httpEnum';
import { isString } from '/@/utils/is';
import { getToken } from '/@/utils/auth';
import { setObjToUrlParams, deepMerge } from '/@/utils';
import { useErrorLogStoreWithOut } from '/@/store/modules/errorLog';
import { useI18n } from '/@/hooks/web/useI18n';
import { joinTimestamp, formatRequestDate } from './helper';
import { useUserStoreWithOut } from '/@/store/modules/user';
import { AxiosRetry } from '/@/utils/http/axios/axiosRetry';
import { errorHandler } from './errorHandle';

const globSetting = useGlobSetting();
const urlPrefix = globSetting.urlPrefix;
const { createMessage, createErrorModal } = useMessage();

/**
 * @description: Data processing, easy to distinguish between multiple processing methods
 */
const transform: AxiosTransform = {
  /**
   * @description: Process request data. If the data is not in the expected format, an error can be thrown directly
   */
  transformRequestHook: (res: AxiosResponse<Result>, options: RequestOptions) => {
    const { t } = useI18n();
    const { isTransformResponse, isReturnNativeResponse } = options;
    //Whether to return native response headers This property is used when you need to get a response header
    if (isReturnNativeResponse) {
      return res;
    }
    if (!isTransformResponse) {
      return res.data;
    }
    const { data } = res;
    if (!data) {
      // return '[HTTP] Request has no return value';
      throw new Error(t('sys.api.apiRequestFailed'));
    }
    const { code, data: result, message } = data;

    // TODO The backend returns data in a uniform format code，data, message
    const hasSuccess = (data && !Reflect.has(data, 'code')) || code === ResultEnum.SUCCESS;
    if (hasSuccess) {
      return result;
    }

    // Do different things on different codes
    let timeoutMsg = '';
    switch (code) {
      case ResultEnum.TIMEOUT:
        timeoutMsg = t('sys.api.timeoutMessage');
        const userStore = useUserStoreWithOut();
        userStore.setToken(undefined);
        userStore.logout(true);
        break;
      default:
        if (message) {
          timeoutMsg = message;
        }
    }

    // errorMessageMode=‘modal’:A modal error pop-up window is displayed instead of a message prompt for some of the more important errors
    // errorMessageMode='none': Generally, when called, it is clearly stated that you do not want the error message to pop up automatically
    if (options.errorMessageMode === 'modal') {
      createErrorModal({ title: t('sys.api.errorTip'), content: timeoutMsg });
    } else if (options.errorMessageMode === 'message') {
      createMessage.error(timeoutMsg);
    }

    throw new Error(timeoutMsg || t('sys.api.apiRequestFailed'));
  },

  // Processing before request
  beforeRequestHook: (config, options) => {
    const { apiUrl, joinPrefix, joinParamsToUrl, formatDate, joinTime = true, urlPrefix } = options;

    if (joinPrefix) {
      config.url = `${urlPrefix}${config.url}`;
    }

    if (apiUrl && isString(apiUrl)) {
      config.url = `${apiUrl}${config.url}`;
    }
    const params = config.params || {};
    const data = config.data || false;
    formatDate && data && !isString(data) && formatRequestDate(data);
    if (config.method?.toUpperCase() === RequestEnum.GET) {
      if (!isString(params)) {
        // Add a timestamp parameter to the get request to avoid taking data from the cache.
        config.params = Object.assign(params || {}, joinTimestamp(joinTime, false));
      } else {
        config.url = config.url + params + `${joinTimestamp(joinTime, true)}`;
        config.params = undefined;
      }
    } else {
      if (!isString(params)) {
        formatDate && formatRequestDate(params);
        if (Reflect.has(config, 'data') && config.data && Object.keys(config.data).length > 0) {
          config.data = data;
          config.params = params;
        } else {
          // Non-GET requests treat params as data if they do not provide data
          config.data = params;
          config.params = undefined;
        }
        if (joinParamsToUrl) {
          config.url = setObjToUrlParams(
            config.url as string,
            Object.assign({}, config.params, config.data),
          );
        }
      } else {
        config.url = config.url + params;
        config.params = undefined;
      }
    }
    return config;
  },

  /**
   * @description: Request interceptor processing
   */
  requestInterceptors: (config, options) => {
    // Processing before request
    const token = getToken();
    if (token && (config as Recordable)?.requestOptions?.withToken !== false) {
      // jwt token
      (config as Recordable).headers.Authorization = options.authenticationScheme
        ? `${options.authenticationScheme} ${token}`
        : token;
    }
    return config;
  },

  /**
   * @description: Response interceptors handle response interceptor processing
   */
  responseInterceptors: (res: AxiosResponse<any>) => {
    return res;
  },

  /**
   * @description: Response error handling
   */
  responseInterceptorsCatch: (axiosInstance: AxiosResponse, error: any) => {
    const { t } = useI18n();
    const errorLogStore = useErrorLogStoreWithOut();
    errorLogStore.addAjaxErrorInfo(error);
    const { response, code, message, config } = error || {};
    const errorMessageMode = config?.requestOptions?.errorMessageMode || 'none';
    // const msg: string = response?.data?.error?.message ?? '';
    const err: string = error?.toString?.() ?? '';
    let errMessage = '';
    try {
      if (code === 'ECONNABORTED' && message.indexOf('timeout') !== -1) {
        errMessage = t('sys.api.apiTimeoutMessage');
      }
      if (err?.includes('Network Error')) {
        errMessage = t('sys.api.networkExceptionMsg');
      }
      if (errMessage) {
        if (errorMessageMode === 'modal') {
          createErrorModal({ title: t('sys.api.errorTip'), content: errMessage });
        } else if (errorMessageMode === 'message') {
          createMessage.error(errMessage);
        }
        return Promise.reject(error);
      }
    } catch (error) {
      throw new Error(error as unknown as string);
    }
    errorHandler(response);

    // Added an automatic retry mechanism for insurance purposes only for GET requests
    const retryRequest = new AxiosRetry();
    const { isOpenRetry } = config.requestOptions.retryRequest;
    config.method?.toUpperCase() === RequestEnum.GET &&
      isOpenRetry &&
      // @ts-ignore
      retryRequest.retry(axiosInstance, error);
    return Promise.reject(error);
  },
};

function createAxios(opt?: Partial<CreateAxiosOptions>) {
  return new VAxios(
    // Deep merge
    deepMerge(
      {
        // See https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#authentication_schemes
        // authentication schemes，e.g: Bearer
        // authenticationScheme: 'Bearer',
        authenticationScheme: '',
        timeout: 30 * 1000,
        withCredentials: false,
        responseType: 'json',

        // baseURL: globSetting.apiUrl,

        headers: { 'Content-Type': ContentTypeEnum.FORM_URLENCODED },
        // If it is in form-data format
        // headers: { 'Content-Type': ContentTypeEnum.FORM_URLENCODED },
        // Data processing method
        transform: clone(transform),
        // Configuration items, the following options can be overridden in standalone interface requests
        requestOptions: {
          // By default, prefix is added to the URL
          joinPrefix: true,
          // Whether to return native response headers This property is used when you need to get a response header
          isReturnNativeResponse: false,
          // The returned data needs to be processed
          isTransformResponse: true,
          // Add parameters to url when posting a request
          joinParamsToUrl: false,
          // Format the submission parameter time
          formatDate: true,
          // The message prompt type
          errorMessageMode: 'message',
          // Address of the interface
          apiUrl: globSetting.apiUrl,
          // Interface stitching address
          urlPrefix: urlPrefix,
          //  Whether to add a timestamp
          joinTime: true,
          // Duplicate requests are ignored
          ignoreCancelToken: true,
          // Whether to carry a token
          withToken: true,
          retryRequest: {
            isOpenRetry: true,
            count: 5,
            waitTime: 100,
          },
        },
      },
      opt || {},
    ),
  );
}
export const defHttp = createAxios();

// other api url
// export const otherHttp = createAxios({
//   requestOptions: {
//     apiUrl: 'xxx',
//     urlPrefix: 'xxx',
//   },
// });
