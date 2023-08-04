package com.aliyun.dts.deliver.core.runtime.tasks;

import com.aliyun.dts.deliver.DtsContext;
import com.aliyun.dts.deliver.commons.functional.SwallowException;
import com.aliyun.dts.deliver.feature.Configurable;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public abstract class Task implements Runnable, Configurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Task.class);

    private static final AtomicLong TASK_ID_GENERATOR = new AtomicLong(0);

    private final long id;
    protected final String taskName;

    private volatile boolean isStopped;
    protected volatile Thread currentThread;

    private Consumer<Task> stopHandler;
    private Pair<Consumer<Task>, Task> revokedStopHandlerPair;

    private Throwable error;
    private volatile boolean quiet;

    private volatile CountDownLatch taskFinishedLatch;

    private TaskStateInfo taskStateInfo;

    protected DtsContext context;

    public Task(String taskName) {
        this(taskName, TASK_ID_GENERATOR.incrementAndGet());
    }

    public Task(String taskName, long id) {
        this.id = id;
        this.taskName = composeTaskNameWithId(taskName);
        reset();
    }

    public long getId() {
        return id;
    }

    public String getTaskName() {
        return taskName;
    }

    public synchronized void reset() {
        isStopped = false;
        currentThread = null;
        stopHandler = null;
        revokedStopHandlerPair = null;
        error = null;
        quiet = false;
        //taskStateInfo = new TaskStateInfo();
    }

    public synchronized void stop() {
        isStopped = true;
        inState("Stop");
    }

    public boolean isQuiet() {
        return quiet;
    }

    public void setStopHandler(Consumer<Task> stopHandler) {
        if (null != this.stopHandler) {
            this.stopHandler = stopHandler.andThen(this.stopHandler);
        } else {
            this.stopHandler = stopHandler;
        }
    }

    public synchronized boolean revokeStopHandler(final Task another) {
        if (isStopped()) {
            return false;
        }

        revokedStopHandlerPair = Pair.of(another.stopHandler, another);
        another.stopHandler = null;

        return true;
    }

    @Override
    public final void run() {
        taskFinishedLatch = new CountDownLatch(1);
        currentThread = Thread.currentThread();
        final String oldThreadName = currentThread.getName();

        try {
            currentThread.setName(getTaskName());

            try {
                safeRun();
            } catch (Throwable e) {
                if (!isQuiet()) {
                    LOGGER.error("task {} failed", getTaskName(), e);
                }
                error = e;
            }

            synchronized (this) {
                currentThread = null;
            }
        } finally {

            stop();

            LOGGER.info("task {} stopped, begin to call stop handler", getTaskName());

            SwallowException.callAndSwallowException(() -> {
                if (null != stopHandler) {
                    stopHandler.accept(this);
                }
            });

            SwallowException.callAndSwallowException(() -> {
                if (null != revokedStopHandlerPair) {
                    revokedStopHandlerPair.getLeft().accept(revokedStopHandlerPair.getRight());
                }
            });

            LOGGER.info("task {} really stopped, finished to call stop handler", getTaskName());

            Thread.currentThread().setName(oldThreadName);
            taskFinishedLatch.countDown();
        }
    }

    public Throwable getError() {
        if (isQuiet()) {
            return null;
        }

        return error;
    }

    public void markQuiet() {
        quiet = true;
    }

    public synchronized boolean isStopped() {
        return isStopped;
    }

    public void join() {
        if (null != taskFinishedLatch) {
            SwallowException.callAndSwallowException(() -> taskFinishedLatch.await());
        }
    }

    public void interruptIfNeeded() {
        if (null != currentThread) {
            synchronized (this) {
                if (null != currentThread) {
                    currentThread.interrupt();
                }
            }
        }
    }

    @Override
    public void config(Map<String, String> config) {
    }

    public void inState(String state) {
        //taskStateInfo.setStateName(state);
    }

    protected abstract void safeRun() throws Throwable;

    private String composeTaskNameWithId(String name) {
        return composeTaskName(name, Long.toString(id));
    }

    protected static String composeTaskName(String... names) {
        return String.join("-", names);
    }

    public void setContext(DtsContext context) {
        this.context = context;
    }
}
