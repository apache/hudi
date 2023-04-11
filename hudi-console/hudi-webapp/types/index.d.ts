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
declare interface Fn<T = any, R = T> {
  (...arg: T[]): R;
}

declare interface PromiseFn<T = any, R = T> {
  (...arg: T[]): Promise<R>;
}

declare type RefType<T> = T | null;

declare type LabelValueOptions = {
  label: string;
  value: any;
  [key: string]: string | number | boolean;
}[];

declare type EmitType = (event: string, ...args: any[]) => void;

declare type TargetContext = '_self' | '_blank';

declare interface ComponentElRef<T extends HTMLElement = HTMLDivElement> {
  $el: T;
}

declare type ComponentRef<T extends HTMLElement = HTMLDivElement> = ComponentElRef<T> | null;

declare type ElRef<T extends HTMLElement = HTMLDivElement> = Nullable<T>;
