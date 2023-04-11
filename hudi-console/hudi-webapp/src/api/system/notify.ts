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
import { NotifyList } from './model/notifyModel';
import { defHttp } from '/@/utils/http/axios';

interface NotifyParam {
  // Notification Type 1: Abnormal Alarm 2: Notification Message,
  type: number;
  // page number
  pageNum: number;
  // page size
  pageSize: number;
}
enum NOTIFY_API {
  NOTICE = '/metrics/notice',
  DEL = '/metrics/delnotice',
}
/**
 * Get notification list
 * @param {NotifyParam} data
 * @returns {Promise<NotifyList>}
 */
export function fetchNotify(data: NotifyParam): Promise<NotifyList> {
  return defHttp.post({ url: NOTIFY_API.NOTICE, data });
}

/**
 * delete notification
 * @param {number} id notification id,
 * @returns {Promise<NotifyList>}
 */
export function fetchNotifyDelete(id: string): Promise<NotifyList> {
  return defHttp.post({
    url: NOTIFY_API.DEL,
    data: { id },
  });
}
