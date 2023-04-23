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
import type { ComputedRef, Ref } from 'vue';
import type { BasicTableProps } from '../types/table';
import { computed, unref, ref, toRaw } from 'vue';
import { ROW_KEY } from '../const';

export function useTableExpand(
  propsRef: ComputedRef<BasicTableProps>,
  tableData: Ref<Recordable[]>,
  emit: EmitType,
) {
  const expandedRowKeys = ref<string[]>([]);

  const getAutoCreateKey = computed(() => {
    return unref(propsRef).autoCreateKey && !unref(propsRef).rowKey;
  });

  const getRowKey = computed(() => {
    const { rowKey } = unref(propsRef);
    return unref(getAutoCreateKey) ? ROW_KEY : rowKey;
  });

  const getExpandOption = computed(() => {
    const { isTreeTable } = unref(propsRef);
    if (!isTreeTable) return {};

    return {
      expandedRowKeys: unref(expandedRowKeys),
      onExpandedRowsChange: (keys: string[]) => {
        expandedRowKeys.value = keys;
        emit('expanded-rows-change', keys);
      },
    };
  });

  function expandAll() {
    const keys = getAllKeys();
    expandedRowKeys.value = keys;
  }

  function expandRows(keys: string[]) {
    // use row ID expands the specified table row
    const { isTreeTable } = unref(propsRef);
    if (!isTreeTable) return;
    expandedRowKeys.value = [...expandedRowKeys.value, ...keys];
  }

  function getAllKeys(data?: Recordable[]) {
    const keys: string[] = [];
    const { childrenColumnName } = unref(propsRef);
    toRaw(data || unref(tableData)).forEach((item) => {
      keys.push(item[unref(getRowKey) as string]);
      const children = item[childrenColumnName || 'children'];
      if (children?.length) {
        keys.push(...getAllKeys(children));
      }
    });
    return keys;
  }

  function collapseAll() {
    expandedRowKeys.value = [];
  }

  return { getExpandOption, expandAll, expandRows, collapseAll };
}
