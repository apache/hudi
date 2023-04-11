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
// copy from element-plus

import type { CSSProperties, Plugin } from 'vue';

type OptionalKeys<T extends Record<string, unknown>> = {
  [K in keyof T]: T extends Record<K, T[K]> ? never : K;
}[keyof T];

type RequiredKeys<T extends Record<string, unknown>> = Exclude<keyof T, OptionalKeys<T>>;

type MonoArgEmitter<T, Keys extends keyof T> = <K extends Keys>(evt: K, arg?: T[K]) => void;

type BiArgEmitter<T, Keys extends keyof T> = <K extends Keys>(evt: K, arg: T[K]) => void;

export type EventEmitter<T extends Record<string, unknown>> = MonoArgEmitter<T, OptionalKeys<T>> &
  BiArgEmitter<T, RequiredKeys<T>>;

export type AnyFunction<T> = (...args: any[]) => T;

export type PartialReturnType<T extends (...args: unknown[]) => unknown> = Partial<ReturnType<T>>;

export type SFCWithInstall<T> = T & Plugin;

export type Nullable<T> = T | null;

export type RefElement = Nullable<HTMLElement>;

export type CustomizedHTMLElement<T> = HTMLElement & T;

export type Indexable<T> = {
  [key: string]: T;
};

export type Hash<T> = Indexable<T>;

export type TimeoutHandle = ReturnType<typeof global.setTimeout>;

export type ComponentSize = 'large' | 'medium' | 'small' | 'mini';

export type StyleValue = string | CSSProperties | Array<StyleValue>;

export type Mutable<T> = { -readonly [P in keyof T]: T[P] };
