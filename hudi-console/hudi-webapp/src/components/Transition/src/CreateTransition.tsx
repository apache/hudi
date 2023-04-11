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

import { defineComponent, Transition, TransitionGroup } from 'vue';
import { getSlot } from '/@/utils/helper/tsxHelper';

type Mode = 'in-out' | 'out-in' | 'default' | undefined;

export function createSimpleTransition(name: string, origin = 'top center 0', mode?: Mode) {
  return defineComponent({
    name,
    props: {
      group: {
        type: Boolean as PropType<boolean>,
        default: false,
      },
      mode: {
        type: String as PropType<Mode>,
        default: mode,
      },
      origin: {
        type: String as PropType<string>,
        default: origin,
      },
    },
    setup(props, { slots, attrs }) {
      const onBeforeEnter = (el: HTMLElement) => {
        el.style.transformOrigin = props.origin;
      };

      return () => {
        const Tag = !props.group ? Transition : TransitionGroup;
        return (
          <Tag name={name} mode={props.mode} {...attrs} onBeforeEnter={onBeforeEnter}>
            {() => getSlot(slots)}
          </Tag>
        );
      };
    },
  });
}
export function createJavascriptTransition(
  name: string,
  functions: Recordable,
  mode: Mode = 'in-out',
) {
  return defineComponent({
    name,
    props: {
      mode: {
        type: String as PropType<Mode>,
        default: mode,
      },
    },
    setup(props, { attrs, slots }) {
      return () => {
        return (
          <Transition
            name={name}
            mode={props.mode}
            {...attrs}
            onBeforeEnter={functions.beforeEnter}
            onEnter={functions.enter}
            onLeave={functions.leave}
            onAfterLeave={functions.afterLeave}
            onLeaveCancelled={functions.afterLeave}
          >
            {() => getSlot(slots)}
          </Transition>
        );
      };
    },
  });
}
