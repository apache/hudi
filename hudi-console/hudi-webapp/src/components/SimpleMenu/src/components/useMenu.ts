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
import { computed, ComponentInternalInstance, unref } from 'vue';
import type { CSSProperties } from 'vue';

export function useMenuItem(instance: ComponentInternalInstance | null) {
  const getParentMenu = computed(() => {
    return findParentMenu(['Menu', 'SubMenu']);
  });

  const getParentRootMenu = computed(() => {
    return findParentMenu(['Menu']);
  });

  const getParentSubMenu = computed(() => {
    return findParentMenu(['SubMenu']);
  });

  const getItemStyle = computed((): CSSProperties => {
    let parent = instance?.parent;
    if (!parent) return {};
    const indentSize = (unref(getParentRootMenu)?.props.indentSize as number) ?? 20;
    let padding = indentSize;

    if (unref(getParentRootMenu)?.props.collapse) {
      padding = indentSize;
    } else {
      while (parent && parent.type.name !== 'Menu') {
        if (parent.type.name === 'SubMenu') {
          padding += indentSize;
        }
        parent = parent.parent;
      }
    }
    return { paddingLeft: padding + 'px' };
  });

  function findParentMenu(name: string[]) {
    let parent = instance?.parent;
    if (!parent) return null;
    while (parent && name.indexOf(parent.type.name!) === -1) {
      parent = parent.parent;
    }
    return parent;
  }

  function getParentList() {
    let parent = instance;
    if (!parent)
      return {
        uidList: [],
        list: [],
      };
    const ret: any[] = [];
    while (parent && parent.type.name !== 'Menu') {
      if (parent.type.name === 'SubMenu') {
        ret.push(parent);
      }
      parent = parent.parent;
    }
    return {
      uidList: ret.map((item) => item.uid),
      list: ret,
    };
  }

  function getParentInstance(instance: ComponentInternalInstance, name = 'SubMenu') {
    let parent = instance.parent;
    while (parent) {
      if (parent.type.name !== name) {
        return parent;
      }
      parent = parent.parent;
    }
    return parent;
  }

  return {
    getParentMenu,
    getParentInstance,
    getParentRootMenu,
    getParentList,
    getParentSubMenu,
    getItemStyle,
  };
}
