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
// Used to configure the general configuration of some components without modifying the components

import type { SorterResult } from '../components/Table';

export default {
  // basic-table setting
  table: {
    // Form interface request general configuration
    // support xxx.xxx.xxx
    fetchSetting: {
      // The field name of the current page passed to the background
      pageField: 'pageNum',
      // The number field name of each page displayed in the background
      sizeField: 'pageSize',
      // Field name of the form data returned by the interface
      listField: 'records',
      // Total number of tables returned by the interface field name
      totalField: 'total',
    },
    // Number of pages that can be selected
    pageSizeOptions: ['10', '50', '80', '100'],
    // Default display quantity on one page
    defaultPageSize: 10,
    // Default Size
    defaultSize: 'small',
    // Custom general sort function
    defaultSortFn: (sortInfo: SorterResult) => {
      const { field, order } = sortInfo;
      if (field && order) {
        return {
          // The sort field passed to the backend you
          sortField: field.replace(/([a-z])([A-Z])/, '$1_$2').toLowerCase(),
          // Sorting method passed to the background asc/desc
          sortOrder: order === 'ascend' ? 'asc' : 'desc',
        };
      } else {
        return {};
      }
    },
    // Custom general filter function
    defaultFilterFn: (data: Partial<Recordable<string[]>>) => {
      return data;
    },
  },
  // scrollbar setting
  scrollbar: {
    // Whether to use native scroll bar
    // After opening, the menu, modal, drawer will change the pop-up scroll bar to native
    native: false,
  },
};
