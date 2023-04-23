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
import type { InjectionKey, ComputedRef } from 'vue';
import { createContext, useContext } from '/@/hooks/core/useContext';

export interface ContentContextProps {
  contentHeight: ComputedRef<number>;
  setPageHeight: (height: number) => Promise<void>;
}

const key: InjectionKey<ContentContextProps> = Symbol();

export function createContentContext(context: ContentContextProps) {
  return createContext<ContentContextProps>(context, key, { native: true });
}

export function useContentContext() {
  return useContext<ContentContextProps>(key);
}
