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
import type { LockInfo, UserInfo } from '/#/store';
import type { ProjectConfig } from '/#/config';
import type { RouteLocationNormalized } from 'vue-router';

import { createLocalStorage, createSessionStorage } from '/@/utils/cache/index';
import { Memory } from './memory';
import {
  TOKEN_KEY,
  USER_INFO_KEY,
  ROLES_KEY,
  LOCK_INFO_KEY,
  PROJ_CFG_KEY,
  APP_LOCAL_CACHE_KEY,
  APP_SESSION_CACHE_KEY,
  MULTIPLE_TABS_KEY,
  EXPIRE_KEY,
  PERMISSION_KEY,
} from '/@/enums/cacheEnum';
import { DEFAULT_CACHE_TIME } from '/@/settings/encryptionSetting';
import { toRaw } from 'vue';
import { pick, omit } from 'lodash-es';

interface BasicStore {
  [TOKEN_KEY]: string | number | null | undefined;
  [EXPIRE_KEY]: string | number | null | undefined;
  [USER_INFO_KEY]: UserInfo;
  [ROLES_KEY]: string[];
  [LOCK_INFO_KEY]: LockInfo;
  [PROJ_CFG_KEY]: ProjectConfig;
  [MULTIPLE_TABS_KEY]: RouteLocationNormalized[];
  [PERMISSION_KEY]: string[];
}

type LocalStore = BasicStore;

type SessionStore = BasicStore;

export type BasicKeys = keyof BasicStore;
type LocalKeys = keyof LocalStore;
type SessionKeys = keyof SessionStore;

const ls = createLocalStorage();
const ss = createSessionStorage();

const localMemory = new Memory(DEFAULT_CACHE_TIME);
const sessionMemory = new Memory(DEFAULT_CACHE_TIME);

function initPersistentMemory() {
  const localCache = ls.get(APP_LOCAL_CACHE_KEY);
  const sessionCache = ss.get(APP_SESSION_CACHE_KEY);
  localCache && localMemory.resetCache(localCache);
  sessionCache && sessionMemory.resetCache(sessionCache);
}

export class Persistent {
  static getLocal<T>(key: LocalKeys) {
    return localMemory.get(key)?.value as Nullable<T>;
  }

  static setLocal(key: LocalKeys, value: LocalStore[LocalKeys], immediate = false): void {
    localMemory.set(key, toRaw(value));
    immediate && ls.set(APP_LOCAL_CACHE_KEY, localMemory.getCache);
  }

  static removeLocal(key: LocalKeys, immediate = false): void {
    localMemory.remove(key);
    immediate && ls.set(APP_LOCAL_CACHE_KEY, localMemory.getCache);
  }

  static clearLocal(immediate = false): void {
    localMemory.clear();
    immediate && ls.clear();
  }

  static getSession<T>(key: SessionKeys) {
    return sessionMemory.get(key)?.value as Nullable<T>;
  }

  static setSession(key: SessionKeys, value: SessionStore[SessionKeys], immediate = false): void {
    sessionMemory.set(key, toRaw(value));
    immediate && ss.set(APP_SESSION_CACHE_KEY, sessionMemory.getCache);
  }

  static removeSession(key: SessionKeys, immediate = false): void {
    sessionMemory.remove(key);
    immediate && ss.set(APP_SESSION_CACHE_KEY, sessionMemory.getCache);
  }
  static clearSession(immediate = false): void {
    sessionMemory.clear();
    immediate && ss.clear();
  }

  static clearAll(immediate = false) {
    sessionMemory.clear();
    localMemory.clear();
    if (immediate) {
      ls.clear();
      ss.clear();
    }
  }
}

window.addEventListener('beforeunload', function () {
  // TOKEN_KEY It has been written to storage when logging in or logging out, and here is to solve the problem that the token is out of sync when multiple windows are open at the same time
  // LOCK_INFO_KEY Written on lock screen and unlock, it should not be modified here either
  ls.set(APP_LOCAL_CACHE_KEY, {
    ...omit(localMemory.getCache, LOCK_INFO_KEY),
    ...pick(ls.get(APP_LOCAL_CACHE_KEY), [TOKEN_KEY, USER_INFO_KEY, LOCK_INFO_KEY]),
  });
  ss.set(APP_SESSION_CACHE_KEY, {
    ...omit(sessionMemory.getCache, LOCK_INFO_KEY),
    ...pick(ss.get(APP_SESSION_CACHE_KEY), [TOKEN_KEY, USER_INFO_KEY, LOCK_INFO_KEY]),
  });
});

function storageChange(e: any) {
  const { key, newValue, oldValue } = e;
  if (!key) {
    Persistent.clearAll();
    return;
  }
  if (!!newValue && !!oldValue) {
    if (APP_LOCAL_CACHE_KEY === key) {
      Persistent.clearLocal();
    }
    if (APP_SESSION_CACHE_KEY === key) {
      Persistent.clearSession();
    }
  }
}

window.addEventListener('storage', storageChange);

initPersistentMemory();
