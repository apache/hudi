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
import type { ColEx } from '../types';
//import type { ButtonProps } from 'ant-design-vue/es/button/buttonTypes';
import { defineComponent, computed, PropType } from 'vue';
import { Form, Col } from 'ant-design-vue';
import { Button, ButtonProps } from '/@/components/Button';
import { BasicArrow } from '/@/components/Basic';
import { useFormContext } from '../hooks/useFormContext';
import { useI18n } from '/@/hooks/web/useI18n';
import { propTypes } from '/@/utils/propTypes';
import { getSlot } from '/@/utils/helper/tsxHelper';

type ButtonOptions = Partial<ButtonProps> & { text: string };

export default defineComponent({
  name: 'BasicFormAction',
  components: {
    FormItem: Form.Item,
    Button,
    BasicArrow,
    // ClearOutlined,
    // SearchOutlined,
    [Col.name]: Col,
  },
  props: {
    showActionButtonGroup: propTypes.bool.def(true),
    showResetButton: propTypes.bool.def(true),
    showSubmitButton: propTypes.bool.def(true),
    showAdvancedButton: propTypes.bool.def(true),
    submitBeforeReset: propTypes.bool.def(false),
    resetButtonOptions: {
      type: Object as PropType<ButtonOptions>,
      default: () => ({}),
    },
    submitButtonOptions: {
      type: Object as PropType<ButtonOptions>,
      default: () => ({}),
    },
    actionColOptions: {
      type: Object as PropType<Partial<ColEx>>,
      default: () => ({}),
    },
    actionSpan: propTypes.number.def(6),
    isAdvanced: propTypes.bool,
    hideAdvanceBtn: propTypes.bool,
  },
  emits: ['toggle-advanced'],
  setup(props, { slots, emit }) {
    const { t } = useI18n();

    const actionColOpt = computed(() => {
      const { showAdvancedButton, actionSpan: span, actionColOptions } = props;
      const actionSpan = 24 - span;
      const advancedSpanObj = showAdvancedButton ? { span: actionSpan < 6 ? 24 : actionSpan } : {};
      const actionColOpt: Partial<ColEx> = {
        style: { textAlign: 'right' },
        span: showAdvancedButton ? 6 : 4,
        ...advancedSpanObj,
        ...actionColOptions,
      };
      return actionColOpt;
    });

    const getResetBtnOptions = computed((): ButtonOptions => {
      return Object.assign({ text: t('common.resetText') }, props.resetButtonOptions);
    });

    const getSubmitBtnOptions = computed(() => {
      return Object.assign({ text: t('common.queryText') }, props.submitButtonOptions);
    });

    function toggleAdvanced() {
      emit('toggle-advanced');
    }
    const { submitAction, resetAction } = useFormContext();
    const renderResetButton = () => {
      return (
        <>
          {getSlot(slots, 'resetBefore')}
          <Button type="default" class="mr-2" {...getResetBtnOptions.value} onClick={resetAction}>
            {getResetBtnOptions.value.text}
          </Button>
        </>
      );
    };
    const renderSubmitButton = () => {
      return (
        <>
          {getSlot(slots, 'submitBefore')}
          <Button type="primary" class="mr-2" {...getSubmitBtnOptions.value} onClick={submitAction}>
            {getSubmitBtnOptions.value.text}
          </Button>
        </>
      );
    };

    function renderAdvanceButton() {
      return (
        <>
          {getSlot(slots, 'advanceBefore')}
          <Button type="link" size="small" class="mr-2" onClick={toggleAdvanced}>
            {props.isAdvanced ? t('component.form.putAway') : t('component.form.unfold')}
          </Button>
          {getSlot(slots, 'advanceAfter')}
        </>
      );
    }

    function getAdvanceGroup() {
      if (props.submitBeforeReset) {
        return (
          <>
            {props.showSubmitButton && renderSubmitButton()}
            {props.showResetButton && renderResetButton()}
          </>
        );
      } else {
        return (
          <>
            {props.showResetButton && renderResetButton()}
            {props.showSubmitButton && renderSubmitButton()}
          </>
        );
      }
    }
    return () => {
      if (props.showActionButtonGroup) {
        return (
          <Col {...actionColOpt.value}>
            <div class="w-full" style={{ textAlign: actionColOpt.value.style.textAlign }}>
              <Form.Item>
                {getAdvanceGroup()}
                {props.showAdvancedButton && !props.hideAdvanceBtn && renderAdvanceButton()}
              </Form.Item>
            </div>
          </Col>
        );
      } else {
        return <div></div>;
      }
    };
  },
});
