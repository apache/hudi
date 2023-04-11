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
import type { FunctionalComponent } from 'vue';
import type { RouteLocation } from 'vue-router';

export interface DefaultContext {
  Component: FunctionalComponent & { type: Recordable };
  route: RouteLocation;
}

export function getTransitionName({
  route,
  openCache,
  cacheTabs,
  enableTransition,
  def,
}: Pick<DefaultContext, 'route'> & {
  enableTransition: boolean;
  openCache: boolean;
  def: string;
  cacheTabs: string[];
}): string | undefined {
  if (!enableTransition) {
    return undefined;
  }

  const isInCache = cacheTabs.includes(route.name as string);
  const transitionName = 'fade-slide';
  let name: string | undefined = transitionName;

  if (openCache) {
    name = isInCache && route.meta.loaded ? transitionName : undefined;
  }
  return name || (route.meta.transitionName as string) || def;
}
