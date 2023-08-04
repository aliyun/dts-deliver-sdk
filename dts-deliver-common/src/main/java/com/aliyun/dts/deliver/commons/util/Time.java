package com.aliyun.dts.deliver.commons.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * Utility methods for getting the time and computing intervals.
 */
public class Time {
    private static Time instance;

    private static final TimeZone UTC_ZONE = TimeZone.getTimeZone("UTC");

    private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT =
        new ThreadLocal<SimpleDateFormat>() {
            @Override
            protected SimpleDateFormat initialValue() {
                return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSSZ");
            }
        };

    private static volatile long approximateMillis = System.currentTimeMillis();

    private static int fakeTimes = 0;
    private static int fakeInterval = 100;
    private static Timer timer = new Timer();

    static {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                fakeTimes++;
                if (fakeTimes > 5) {
                    fakeTimes = 0;
                    approximateMillis = System.currentTimeMillis();
                }  else {
                    approximateMillis += fakeInterval;
                }
            }
        }, 0, fakeInterval);
    }

    protected Time() {
    }

    public static void stop() {
        timer.cancel();
        timer.purge();
    }

    /**
     * Current system time.  Do not use this to calculate a duration or interval
     * to sleep, because it will be broken by settimeofday.  Instead, use
     * monotonicNow.
     *
     * @return current time in msec.
     */
    public static long now() {
        return System.currentTimeMillis();
    }

    /**
     * Current time from some arbitrary time base in the past, counting in
     * milliseconds, and not affected by settimeofday or similar system clock
     * changes.  This is appropriate to use when computing how much longer to
     * wait for an interval to expire.
     * This function can return a negative value and it must be handled correctly
     * by callers. See the documentation of System#nanoTime for caveats.
     *
     * @return a monotonic clock that counts in milliseconds.
     */
    public static long monotonicNow() {
        return TimeUnit.NANOSECONDS.toMillis(
            System.nanoTime());
    }

    /**
     * Same as {@link #monotonicNow()} but returns its result in nanoseconds.
     * Note that this is subject to the same resolution constraints as
     * {@link System#nanoTime()}.
     *
     * @return a monotonic clock that counts in nanoseconds.
     */
    public static long monotonicNowNanos() {
        return System.nanoTime();
    }

    /**
     * Convert time in millisecond to human readable format.
     *
     * @return a human readable string for the input time
     */
    public static String formatTime(long millis) {
        return DATE_FORMAT.get().format(millis);
    }

    /**
     * Get the current UTC time in milliseconds.
     *
     * @return the current UTC time in milliseconds.
     */
    public static long getUtcTime() {
        return Calendar.getInstance(UTC_ZONE).getTimeInMillis();
    }

    public static Time getInstance() {
        if (null == instance) {
            synchronized (Time.class) {
                if (null == instance) {
                    instance = new Time();
                }
            }
        }
        return instance;
    }

    public long getApproximateCurrentMillis() {
        return approximateMillis;
    }
}
