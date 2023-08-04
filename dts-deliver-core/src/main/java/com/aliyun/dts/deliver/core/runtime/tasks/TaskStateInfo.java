package com.aliyun.dts.deliver.core.runtime.tasks;

import com.aliyun.dts.deliver.commons.util.Time;
import org.apache.commons.lang3.StringUtils;

public class TaskStateInfo {

    private String stateName;
    private long stateStartMillSecond;

    public TaskStateInfo() {
        stateName = "Idle";
        stateStartMillSecond = Time.now();
    }

    public void setStateName(String stateName) {
        if (!StringUtils.equals(this.stateName, stateName)) {
            this.stateName = stateName;
            stateStartMillSecond = Time.now();
        }
    }

    public String getStateName() {
        return stateName;
    }

    public long getStateStartMillSecond() {
        return stateStartMillSecond;
    }

    public long getStateDurationMillSecond() {
        long now = Time.now();
        return now - stateStartMillSecond;
    }
}
