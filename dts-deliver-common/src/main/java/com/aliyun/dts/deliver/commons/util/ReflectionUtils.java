package com.aliyun.dts.deliver.commons.util;

/**
 *  (C) 2010-2014 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ReflectionUtils {
    private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = new ConcurrentHashMap<>();
    private static final ThreadMXBean THREAD_BEAN = ManagementFactory.getThreadMXBean();

    public static <T> T newInstance(Class<T> theClass) {
        T result;
        try {
            Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
            if (meth == null) {
                meth = theClass.getDeclaredConstructor(null);
                meth.setAccessible(true);
                CONSTRUCTOR_CACHE.put(theClass, meth);
            }
            result = meth.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    public static <T> T newInstance(String cls_name) {
        if (cls_name == null || cls_name.length() <= 0) {
            return null;
        }

        try {
            Class<T> cls = (Class<T>) Class.forName(cls_name);
            return newInstance(cls);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static <T> T newInstance(String cls_name, Class caller) {
        if (cls_name == null || cls_name.length() <= 0) {
            return null;
        }

        try {
            Class<T> cls = (Class<T>) Class.forName(cls_name, true, caller.getClassLoader());
            return newInstance(cls);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static <T> T getPrivateMemberValue(Class memberHolderClass, Object memberHolder, String memberName) throws IllegalAccessException {
        if (null == memberHolderClass) {
            return null;
        }

        for (Field filed : memberHolderClass.getDeclaredFields()) {

            if (memberName.equals(filed.getName())) {
                filed.setAccessible(true);
                return (T) filed.get(memberHolder);
            }
        }

        return getPrivateMemberValue(memberHolderClass.getSuperclass(), memberHolder, memberName);
    }

    public static <T> T getPrivateMemberValue(Object memberHolder, String memberName) throws IllegalAccessException {
        return getPrivateMemberValue(memberHolder.getClass(), memberHolder, memberName);
    }

    public static <T> void setField(Class clazz, Object obj, String fieldName, T value) throws NoSuchFieldException, IllegalAccessException {
        Field name = clazz.getDeclaredField(fieldName);
        name.setAccessible(true);
        name.set(obj, value);
        name.setAccessible(false);
    }

    public static <T> T call(Class clazz, Object obj, String methodName, Object... params)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Method[] methods = clazz.getDeclaredMethods();
        Method method = null;

        for (int i = 0; i < methods.length; i++) {
            if (StringUtils.equals(methods[i].getName(), methodName)) {
                method = methods[i];
                break;
            }
        }

        if (null == method) {
            throw new NoSuchMethodException();
        }

        method.setAccessible(true);
        return (T) method.invoke(obj, params);
    }

    private static final Class<?>[] EMPTY_ARRAY = new Class[]{};

    public static void setContentionTracing(boolean val) {
        THREAD_BEAN.setThreadContentionMonitoringEnabled(val);
    }

    private static String getTaskName(long id, String name) {
        if (name == null) {
            return Long.toString(id);
        }
        return id + " (" + name + ")";
    }

    /**
     * Print all of the thread's information and stack traces.
     *
     * @param stream the stream to
     * @param title  a string title for the stack trace
     */
    public static synchronized void printThreadInfo(PrintStream stream,
                                                    String title) {
        final int stackDepth = 20;
        boolean contention = THREAD_BEAN.isThreadContentionMonitoringEnabled();
        long[] threadIds = THREAD_BEAN.getAllThreadIds();
        stream.println("Process Thread Dump: " + title);
        stream.println(threadIds.length + " active threads");
        for (long tid : threadIds) {
            ThreadInfo info = THREAD_BEAN.getThreadInfo(tid, stackDepth);
            if (info == null) {
                stream.println("  Inactive");
                continue;
            }
            stream.println("Thread "
                    + getTaskName(info.getThreadId(),
                    info.getThreadName()) + ":");
            Thread.State state = info.getThreadState();
            stream.println("  State: " + state);
            stream.println("  Blocked count: " + info.getBlockedCount());
            stream.println("  Waited count: " + info.getWaitedCount());
            if (contention) {
                stream.println("  Blocked time: " + info.getBlockedTime());
                stream.println("  Waited time: " + info.getWaitedTime());
            }
            if (state == Thread.State.WAITING) {
                stream.println("  Waiting on " + info.getLockName());
            } else if (state == Thread.State.BLOCKED) {
                stream.println("  Blocked on " + info.getLockName());
                stream.println("  Blocked by "
                        + getTaskName(info.getLockOwnerId(),
                        info.getLockOwnerName()));
            }
            stream.println("  Stack:");
            for (StackTraceElement frame : info.getStackTrace()) {
                stream.println("    " + frame.toString());
            }
        }
        stream.flush();
    }

    private static long previousLogTime = 0;

    /**
     * Log the current thread stacks at INFO level.
     *
     * @param log         the logger that logs the stack trace
     * @param title       a descriptive title for the call stacks
     * @param minInterval the minimum time from the last
     */
    public static void logThreadInfo(Logger log,
                                     String title,
                                     long minInterval) {
        boolean dumpStack = false;
        if (log.isInfoEnabled()) {
            synchronized (ReflectionUtils.class) {
                long now = System.currentTimeMillis();
                if (now - previousLogTime >= minInterval * 1000) {
                    previousLogTime = now;
                    dumpStack = true;
                }
            }
            if (dumpStack) {
                try {
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    printThreadInfo(new PrintStream(buffer, false, "UTF-8"), title);
                    log.info(buffer.toString(Charset.defaultCharset().name()));
                } catch (UnsupportedEncodingException ignored) {
                }
            }
        }
    }

    /**
     * Return the correctly-typed {@link Class} of the given object.
     *
     * @param o object whose correctly-typed <code>Class</code> is to be obtained
     * @return the correctly typed <code>Class</code> of the given object.
     */
    @SuppressWarnings("unchecked")
    public static <T> Class<T> getClass(T o) {
        return (Class<T>) o.getClass();
    }

    // methods to support testing
    static void clearCache() {
        CONSTRUCTOR_CACHE.clear();
    }

    static int getCacheSize() {
        return CONSTRUCTOR_CACHE.size();
    }

    /**
     * Gets all the declared fields of a class including fields declared in
     * superclasses.
     */
    public static List<Field> getDeclaredFieldsIncludingInherited(Class<?> clazz) {
        List<Field> fields = new ArrayList<Field>();
        while (clazz != null) {
            for (Field field : clazz.getDeclaredFields()) {
                fields.add(field);
            }
            clazz = clazz.getSuperclass();
        }

        return fields;
    }

    /**
     * Gets all the declared methods of a class including methods declared in
     * superclasses.
     */
    public static List<Method> getDeclaredMethodsIncludingInherited(Class<?> clazz) {
        List<Method> methods = new ArrayList<Method>();
        while (clazz != null) {
            for (Method method : clazz.getDeclaredMethods()) {
                methods.add(method);
            }
            clazz = clazz.getSuperclass();
        }

        return methods;
    }
}
