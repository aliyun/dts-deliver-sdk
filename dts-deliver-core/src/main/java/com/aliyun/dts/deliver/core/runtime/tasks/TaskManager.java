package com.aliyun.dts.deliver.core.runtime.tasks;

import com.aliyun.dts.deliver.commons.exceptions.DtsCoreException;
import com.aliyun.dts.deliver.commons.exceptions.ErrorCode;
import org.apache.kafka.common.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class TaskManager<T extends Task> {
    private static final Logger LOG = LoggerFactory.getLogger(TaskManager.class);

    private final Map<Long, T> runningTasks;
    private final Queue<T> stoppedTasks;
    private final String taskType;

    private final Supplier<Integer> maxRunningTaskSupplier;

    private final Consumer<TaskManager> raiseMoreTaskHandler;

    public TaskManager(Supplier<Integer> maxRunningTaskSupplier, Metrics metrics, String type) {
        this(maxRunningTaskSupplier, null, metrics, type);
    }

    public TaskManager(Supplier<Integer> maxRunningTaskSupplier, Consumer<TaskManager> raiseMoreTaskHandler, Metrics metrics, String type) {
        runningTasks = new ConcurrentHashMap<>();
        stoppedTasks = new ConcurrentLinkedQueue<>();

        this.maxRunningTaskSupplier = maxRunningTaskSupplier;
        this.raiseMoreTaskHandler = raiseMoreTaskHandler;
        this.taskType = type;

        if (null != metrics) {
            metrics.addMetric(metrics.metricName(taskType + "RunningTasksSize", taskType), (config, now) -> runningTasks.size());
            metrics.addMetric(metrics.metricName(taskType + "StoppedTasksSize", taskType), (config, now) -> stoppedTasks.size());
        }
    }

    @SuppressWarnings("unchecked")
    public void addTask(T task) {
        if (!canRaiseMoreTask()) {
            throw new DtsCoreException(ErrorCode.FRAMEWORK_ILLEGAL_STATE, "can not raise more task");
        }

        task.setStopHandler(stoppedTask -> {
            stoppedTasks.add((T) stoppedTask);
            runningTasks.remove(task.getId());
            if (null != raiseMoreTaskHandler) {
                raiseMoreTaskHandler.accept(this);
            }
        });

        runningTasks.put(task.getId(), task);
    }

    public boolean canRaiseMoreTask() {
        int maxTaskNumber = maxRunningTaskSupplier.get();
        if (maxTaskNumber < 1) {
            // max task number is less than 1, which means we can raise unlimited tasks
            return true;
        } else {
            return runningTasks.size() < maxTaskNumber;
        }
    }

    public boolean isAllTaskStopped() {
        if (runningTasks.isEmpty()) {
            return true;
        }

        return false;
    }

    public void stopTasks(boolean force) {
        LOG.info("task manager stopped {}", force ? "by force" : "normally");
        runningTasks.values().forEach(T::stop);

        if (force) {
            runningTasks.values().forEach(T::interruptIfNeeded);
        }
    }

    public void iterateRunningTasks(Consumer<T> taskConsumer) {
        if (!runningTasks.isEmpty()) {
            runningTasks.values().forEach(taskConsumer::accept);
        }
    }

    public T reclaimTask() {
        if (stoppedTasks.isEmpty()) {
            return null;
        }

        return stoppedTasks.poll();
    }

    public String getTaskType() {
        return taskType;
    }

    public Map<Long, T> getRunningTasks() {
        return runningTasks;
    }
}

