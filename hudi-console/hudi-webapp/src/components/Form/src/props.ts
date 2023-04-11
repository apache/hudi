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
import type { FieldMapToTime, FormSchema } from './types/form';
import type { CSSProperties, PropType } from 'vue';
import type { ColEx } from './types';
import type { TableActionType } from '/@/components/Table';
import type { ButtonProps } from 'ant-design-vue/es/button/buttonTypes';
import type { RowProps } from 'ant-design-vue/lib/grid/Row';
import { propTypes } from '/@/utils/propTypes';

export const basicProps = {
  model: {
    type: Object as PropType<Recordable>,
    default: () => ({}),
  },
  // Label width Fixed width
  labelWidth: {
    type: [Number, String] as PropType<number | string>,
    default: 0,
  },
  fieldMapToTime: {
    type: Array as PropType<FieldMapToTime>,
    default: () => [],
  },
  compact: propTypes.bool,
  // Form configuration rules
  schemas: {
    type: [Array] as PropType<FormSchema[]>,
    default: () => [],
  },
  mergeDynamicData: {
    type: Object as PropType<Recordable>,
    default: null,
  },
  baseRowStyle: {
    type: Object as PropType<CSSProperties>,
  },
  baseColProps: {
    type: Object as PropType<Partial<ColEx>>,
  },
  autoSetPlaceHolder: propTypes.bool.def(true),
  // Whether to automatically submit when you click Enter on the INPUT component
  autoSubmitOnEnter: propTypes.bool.def(false),
  submitOnReset: propTypes.bool,
  submitOnChange: propTypes.bool,
  submitBeforeReset: propTypes.bool,
  size: propTypes.oneOf(['default', 'small', 'large']).def('default'),
  // Disable the form
  disabled: propTypes.bool,
  emptySpan: {
    type: [Number, Object] as PropType<number>,
    default: 0,
  },
  // Whether to display the collapse expand button
  showAdvancedButton: propTypes.bool,
  // Conversion time
  transformDateFunc: {
    type: Function as PropType<Fn>,
    default: (date: any) => {
      return date?.format?.('YYYY-MM-DD HH:mm:ss') ?? date;
    },
  },
  rulesMessageJoinLabel: propTypes.bool.def(true),
  // More than 3 rows are automatically collapsed
  autoAdvancedLine: propTypes.number.def(3),
  // The number of rows that are not affected by folding
  alwaysShowLines: propTypes.number.def(1),

  // Whether to display the action button
  showActionButtonGroup: propTypes.bool.def(true),
  // Operation column Col configuration
  actionColOptions: Object as PropType<Partial<ColEx>>,
  // The reset button is displayed
  showResetButton: propTypes.bool.def(true),
  // Whether to focus on the first input box only works when the first form item is input
  autoFocusFirstItem: propTypes.bool,
  // Reset button configuration
  resetButtonOptions: Object as PropType<Partial<ButtonProps>>,

  showSubmitButton: propTypes.bool.def(true),
  submitButtonOptions: Object as PropType<Partial<ButtonProps>>,

  // Custom reset function
  resetFunc: Function as PropType<() => Promise<void>>,
  submitFunc: Function as PropType<() => Promise<void>>,

  // default props
  hideRequiredMark: propTypes.bool,

  labelCol: Object as PropType<Partial<ColEx>>,

  layout: propTypes.oneOf(['horizontal', 'vertical', 'inline']).def('horizontal'),
  tableAction: {
    type: Object as PropType<TableActionType>,
  },

  wrapperCol: Object as PropType<Partial<ColEx>>,

  colon: propTypes.bool,

  labelAlign: propTypes.string,

  rowProps: Object as PropType<RowProps>,
};
