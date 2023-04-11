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
import { withInstall } from '/@/utils';
import collapseContainer from './src/collapse/CollapseContainer.vue';
import scrollContainer from './src/ScrollContainer.vue';
import lazyContainer from './src/LazyContainer.vue';

export const CollapseContainer = withInstall(collapseContainer);
export const ScrollContainer = withInstall(scrollContainer);
export const LazyContainer = withInstall(lazyContainer);

export * from './src/typing';
