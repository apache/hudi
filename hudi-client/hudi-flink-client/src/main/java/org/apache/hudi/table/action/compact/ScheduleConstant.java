package org.apache.hudi.table.action.compact;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author ez
 * @Description
 * @createTime 2022/3/17
 */
public class ScheduleConstant {
    static AtomicInteger countDeltaCommit = new AtomicInteger(0);
}
