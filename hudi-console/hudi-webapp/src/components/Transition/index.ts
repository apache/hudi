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
import { createSimpleTransition, createJavascriptTransition } from './src/CreateTransition';

import ExpandTransitionGenerator from './src/ExpandTransition';

export { default as CollapseTransition } from './src/CollapseTransition.vue';

export const FadeTransition = createSimpleTransition('fade-transition');
export const ScaleTransition = createSimpleTransition('scale-transition');
export const SlideYTransition = createSimpleTransition('slide-y-transition');
export const ScrollYTransition = createSimpleTransition('scroll-y-transition');
export const SlideYReverseTransition = createSimpleTransition('slide-y-reverse-transition');
export const ScrollYReverseTransition = createSimpleTransition('scroll-y-reverse-transition');
export const SlideXTransition = createSimpleTransition('slide-x-transition');
export const ScrollXTransition = createSimpleTransition('scroll-x-transition');
export const SlideXReverseTransition = createSimpleTransition('slide-x-reverse-transition');
export const ScrollXReverseTransition = createSimpleTransition('scroll-x-reverse-transition');
export const ScaleRotateTransition = createSimpleTransition('scale-rotate-transition');

export const ExpandXTransition = createJavascriptTransition(
  'expand-x-transition',
  ExpandTransitionGenerator('', true),
);

export const ExpandTransition = createJavascriptTransition(
  'expand-transition',
  ExpandTransitionGenerator(''),
);
