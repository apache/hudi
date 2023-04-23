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
import type { PropType } from 'vue';
import type { PaginationProps } from './types/pagination';
import type {
  BasicColumn,
  FetchSetting,
  TableSetting,
  SorterResult,
  TableCustomRecord,
  TableRowSelection,
  SizeType,
} from './types/table';
import type { FormProps } from '/@/components/Form';

import { DEFAULT_FILTER_FN, DEFAULT_SORT_FN, FETCH_SETTING, DEFAULT_SIZE } from './const';
import { propTypes } from '/@/utils/propTypes';

export const basicProps = {
  clickToRowSelect: { type: Boolean, default: true },
  isTreeTable: Boolean,
  tableSetting: propTypes.shape<TableSetting>({}),
  inset: Boolean,
  sortFn: {
    type: Function as PropType<(sortInfo: SorterResult) => any>,
    default: DEFAULT_SORT_FN,
  },
  filterFn: {
    type: Function as PropType<(data: Partial<Recordable<string[]>>) => any>,
    default: DEFAULT_FILTER_FN,
  },
  showTableSetting: Boolean,
  autoCreateKey: { type: Boolean, default: true },
  striped: { type: Boolean, default: true },
  showSummary: Boolean,
  summaryFunc: {
    type: [Function, Array] as PropType<(...arg: any[]) => any[]>,
    default: null,
  },
  summaryData: {
    type: Array as PropType<Recordable[]>,
    default: null,
  },
  indentSize: propTypes.number.def(24),
  canColDrag: { type: Boolean, default: true },
  api: {
    type: Function as PropType<(...arg: any[]) => Promise<any>>,
    default: null,
  },
  beforeFetch: {
    type: Function as PropType<Fn>,
    default: null,
  },
  afterFetch: {
    type: Function as PropType<Fn>,
    default: null,
  },
  handleSearchInfoFn: {
    type: Function as PropType<Fn>,
    default: null,
  },
  fetchSetting: {
    type: Object as PropType<FetchSetting>,
    default: () => {
      return FETCH_SETTING;
    },
  },
  // Request the interface now
  immediate: { type: Boolean, default: true },
  emptyDataIsShowTable: { type: Boolean, default: true },
  // Additional request parameters
  searchInfo: {
    type: Object as PropType<Recordable>,
    default: null,
  },
  // The default sort parameter
  defSort: {
    type: Object as PropType<Recordable>,
    default: null,
  },
  // Use the search form
  useSearchForm: propTypes.bool,
  formConfig: {
    type: Object as PropType<Partial<FormProps>>,
    default: null,
  },
  columns: {
    type: [Array] as PropType<BasicColumn[]>,
    default: () => [],
  },
  showIndexColumn: { type: Boolean, default: true },
  indexColumnProps: {
    type: Object as PropType<BasicColumn>,
    default: null,
  },
  actionColumn: {
    type: Object as PropType<BasicColumn>,
    default: null,
  },
  ellipsis: { type: Boolean, default: true },
  isCanResizeParent: { type: Boolean, default: false },
  canResize: { type: Boolean, default: true },
  clearSelectOnPageChange: propTypes.bool,
  resizeHeightOffset: propTypes.number.def(0),
  rowSelection: {
    type: Object as PropType<TableRowSelection | null>,
    default: null,
  },
  title: {
    type: [String, Function] as PropType<string | ((data: Recordable) => string)>,
    default: null,
  },
  titleHelpMessage: {
    type: [String, Array] as PropType<string | string[]>,
  },
  maxHeight: propTypes.number,
  dataSource: {
    type: Array as PropType<Recordable[]>,
    default: null,
  },
  rowKey: {
    type: [String, Function] as PropType<string | ((record: Recordable) => string)>,
    default: '',
  },
  bordered: propTypes.bool,
  pagination: {
    type: [Object, Boolean] as PropType<PaginationProps | boolean>,
    default: null,
  },
  loading: propTypes.bool,
  rowClassName: {
    type: Function as PropType<(record: TableCustomRecord<any>, index: number) => string>,
  },
  scroll: {
    type: Object as PropType<{ x: number | true; y: number }>,
    default: null,
  },
  beforeEditSubmit: {
    type: Function as PropType<
      (data: {
        record: Recordable;
        index: number;
        key: string | number;
        value: any;
      }) => Promise<any>
    >,
  },
  size: {
    type: String as PropType<SizeType>,
    default: DEFAULT_SIZE,
  },
};
