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
/**
 * Independent time operation tool to facilitate subsequent switch to dayjs
 */
import dayjs from 'dayjs';

const DATE_TIME_FORMAT = 'YYYY-MM-DD HH:mm:ss';
const DATE_FORMAT = 'YYYY-MM-DD';

export function formatToDateTime(
  date: dayjs.ConfigType | undefined = undefined,
  format = DATE_TIME_FORMAT,
): string {
  return dayjs(date).format(format);
}

export function formatToDate(
  date: dayjs.ConfigType | undefined = undefined,
  format = DATE_FORMAT,
): string {
  return dayjs(date).format(format);
}
export function dateToDuration(ms: number) {
  if (ms === 0 || ms === undefined || ms === null) {
    return '';
  }
  const ss = 1000;
  const mi = ss * 60;
  const hh = mi * 60;
  const dd = hh * 24;

  const day = parseInt(ms / dd);
  const hour = parseInt((ms - day * dd) / hh);
  const minute = parseInt((ms - day * dd - hour * hh) / mi);
  const seconds = parseInt((ms - day * dd - hour * hh - minute * mi) / ss);
  if (day > 0) {
    return day + 'd ' + hour + 'h ' + minute + 'm ' + seconds + 's';
  } else if (hour > 0) {
    return hour + 'h ' + minute + 'm ' + seconds + 's';
  } else if (minute > 0) {
    return minute + 'm ' + seconds + 's';
  } else {
    return 0 + 'm ' + seconds + 's';
  }
}
export const dateUtil = dayjs;
