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
import { nextTick, unref } from 'vue';
import { warn } from '/@/utils/log';

export function useTableScrollTo(
  tableElRef: Ref<ComponentRef>,
  getDataSourceRef: ComputedRef<Recordable[]>,
) {
  let bodyEl: HTMLElement | null;

  async function findTargetRowToScroll(targetRowData: Recordable) {
    const { id } = targetRowData;
    const targetRowEl: HTMLElement | null | undefined = bodyEl?.querySelector(
      `[data-row-key="${id}"]`,
    );
    //Add a delay to get new dataSource
    await nextTick();
    bodyEl?.scrollTo({
      top: targetRowEl?.offsetTop ?? 0,
      behavior: 'smooth',
    });
  }

  function scrollTo(pos: string): void {
    const table = unref(tableElRef);
    if (!table) return;

    const tableEl: Element = table.$el;
    if (!tableEl) return;

    if (!bodyEl) {
      bodyEl = tableEl.querySelector('.ant-table-body');
      if (!bodyEl) return;
    }

    const dataSource = unref(getDataSourceRef);
    if (!dataSource) return;

    // judge pos type
    if (pos === 'top') {
      findTargetRowToScroll(dataSource[0]);
    } else if (pos === 'bottom') {
      findTargetRowToScroll(dataSource[dataSource.length - 1]);
    } else {
      const targetRowData = dataSource.find((data) => data.id === pos);
      if (targetRowData) {
        findTargetRowToScroll(targetRowData);
      } else {
        warn(`id: ${pos} doesn't exist`);
      }
    }
  }

  return { scrollTo };
}
