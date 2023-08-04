package com.aliyun.dts.deliver.commons.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TimeUtils {

    private static final ThreadLocal<SimpleDateFormatCache> FORMATTER_CACHE = new ThreadLocal<SimpleDateFormatCache>();

    static class SimpleDateFormatCache {
        private Map<String, SimpleDateFormat> formatters = new HashMap<String, SimpleDateFormat>();

        public SimpleDateFormat getSimpleDateFormat(String format) {
            SimpleDateFormat dateTimeFormat = formatters.get(format);
            if (dateTimeFormat == null) {
                dateTimeFormat = new SimpleDateFormat(format);
                formatters.put(format, dateTimeFormat);
            }
            return dateTimeFormat;
        }
    }

    public static SimpleDateFormat getSimpleDateFormat() {
        return getSimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    public static SimpleDateFormat getSimpleDateFormat(String format) {
        SimpleDateFormatCache cache = FORMATTER_CACHE.get();
        if (cache == null) {
            cache = new SimpleDateFormatCache();
            FORMATTER_CACHE.set(cache);
        }
        return cache.getSimpleDateFormat(format);
    }

    public static SimpleDateFormat getTDateFormat() {
        return getSimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    }

    public static String getTDatetimeStringFromTimestamp(long ts) {
        return getTDateFormat().format(getTimestampMills(ts));
    }

    public static long getTimestampMills(long ts) {
        return ts / 10000000000L > 0 ? ts : ts * 1000;
    }

    public static TimeExpireJudge getTimeExpireJudge(Time time, long duration, TimeUnit durationUnit) {
        return new TimeExpireJudge(time, durationUnit.toMillis(duration));
    }

    public static class TimeExpireJudge {
        private Time time;
        private long startMilli;
        private long stopMilli;
        private final long duration;

        TimeExpireJudge(Time time, long durationMilliSeconds) {
            this.time = time;
            this.duration = durationMilliSeconds;
            this.startMilli = 0;
        }

        public void start() {
            startMilli = time.getApproximateCurrentMillis();
            stopMilli = 0;
        }

        public void stop() {
            stopMilli = time.getApproximateCurrentMillis();
        }

        public void reset() {
            startMilli = 0;
            stopMilli = 0;
        }

        public long getElapsedTime(TimeUnit timeUnit) {
            return timeUnit.convert(getEndMillis() - startMilli, timeUnit);
        }

        private long getEndMillis() {
            if (0 == stopMilli) {
                return time.getApproximateCurrentMillis();
            }
            return stopMilli;
        }

        public boolean isExpired() {
            return (getEndMillis() - startMilli - duration) > 0;
        }
    }

    public static void main(String[] args) throws ParseException {
        String value = "2015-11-11";

        SimpleDateFormat dateTimeFormat1 = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat dateTimeFormat2 = new SimpleDateFormat("yyyyMMdd");
        System.out.println(dateTimeFormat2.format(dateTimeFormat1.parse(value)));
        value = "2015-03-09";
        System.out.println(dateTimeFormat2.format(dateTimeFormat1.parse(value)));

        value = "2015-11-11 12:12:12.1";

        SimpleDateFormat dateTimeFormat3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
        SimpleDateFormat dateTimeFormat4 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S");
        System.out.println(dateTimeFormat4.format(dateTimeFormat3.parse(value)));

        value = "2015-03-09 12:13:25.123456";

        SimpleDateFormat dateTimeFormat5 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
        SimpleDateFormat dateTimeFormat6 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        System.out.println(dateTimeFormat6.format(dateTimeFormat5.parse(value)));
    }
}
