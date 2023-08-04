package com.aliyun.dts.deliver.recordinterceptor.dmlfilter;

import com.google.common.base.Preconditions;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;

public class FilterMetric {
    public static final String FILTER_METRIC_GROUP_NAME = "filerRecordInterceptor";
    private long totalCount;
    private long totalReceiveDML;
    private long totalHintDML;
    private long totalReceiveDDL;
    private long totalHintDDL;
    private long latestRecordTimestamp;
    private final String name;
    private final Metrics metrics;
    public FilterMetric(Metrics metrics, String name) {
        Preconditions.checkState(null != metrics);
        this.metrics = metrics;
        this.name = name;
        initMetrics(metrics, name);
    }

    private void initMetrics(Metrics metrics, String name) {
        String groupName = FILTER_METRIC_GROUP_NAME + "-" + name;
        // init totalCount
        metrics.addMetric(metrics.metricName("totalCount", groupName, "total receive count"), new Measurable() {
            @Override
            public double measure(MetricConfig config, long now) {
                return totalCount;
            }
        });
        // init totalReceiveDML
        metrics.addMetric(metrics.metricName("totalReceiveDML", groupName, "total receive dml counut"), new Measurable() {
            @Override
            public double measure(MetricConfig config, long now) {
                return totalReceiveDML;
            }
        });
        // init totalHintDML
        metrics.addMetric(metrics.metricName("totalHintDML", groupName, "total hint dml count"), new Measurable() {
            @Override
            public double measure(MetricConfig config, long now) {
                return totalHintDML;
            }
        });
        // init totalReceiveDDL
        metrics.addMetric(metrics.metricName("totalReceiveDDL", groupName, "total receive ddl count"), new Measurable() {
            @Override
            public double measure(MetricConfig config, long now) {
                return totalReceiveDDL;
            }
        });
        // init totalHintDDL
        metrics.addMetric(metrics.metricName("totalHintDDl", groupName, "total hint ddl count"), new Measurable() {
            @Override
            public double measure(MetricConfig config, long now) {
                return totalHintDDL;
            }
        });
        // init latestRecordTimestamp
        metrics.addMetric(metrics.metricName("readerTimestamp", groupName, "latest reader timestamp"), new Measurable() {
            @Override
            public double measure(MetricConfig config, long now) {
                return latestRecordTimestamp;
            }
        });
    }

    public void incTotal() {
        ++totalCount;
    }

    public void incReceiveDML() {
        ++totalReceiveDML;
    }

    public void incHintDML() {
        ++totalHintDML;
    }

    public void incReceiveDDL() {
        ++totalReceiveDDL;
    }

    public void incHinDDL() {
        ++totalHintDDL;
    }

    public void setLatestRecordTimestamp(long latestRecordTimestamp) {
        this.latestRecordTimestamp = latestRecordTimestamp;
    }

    public String toString() {
        return "FilterMetric[totalReceive:" + totalCount
                + ",ReceiveDML:" + totalReceiveDML
                + ",HintDML:" + totalHintDML
                + ",ReceiveDDL:" + totalReceiveDDL
                + ",HintDDL:" + totalHintDDL + "]";
    }
}
