package com.aliyun.dts.deliver.core.bootstrap;

import com.aliyun.dts.deliver.DtsContext;
import com.aliyun.dts.deliver.base.Destination;
import com.aliyun.dts.deliver.base.Source;
import com.aliyun.dts.deliver.commons.config.GlobalSettings;
import com.aliyun.dts.deliver.commons.config.JobConfig;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.commons.openapi.DtsOpenApi;
import com.aliyun.dts.deliver.commons.util.ReflectionUtils;
import com.aliyun.dts.deliver.core.runtime.DtsIntegrationRunner;
import com.aliyun.dts.deliver.core.runtime.standalone.StandaloneContext;
import com.aliyun.dts.deliver.protocol.record.checkpoint.RecordCheckpoint;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class DtsDeliver {
    private static final Logger LOG = LoggerFactory.getLogger(DtsDeliver.class);

    /**
     * The settings of this process, you can get all config item from it.
     */
    protected Settings settings;

    private Metrics coreMetrics;
    private Metrics pluginMetrics;

    protected StandaloneContext standaloneContext;

    protected DtsContext dtsContext;

    private JobConfig jobConfig;

    private List<Source> sourceList;

    private Destination destination;

    private Consumer<List<Pair<String, RecordCheckpoint>>> checkpointConsumer;

    private DtsIntegrationRunner integrationRunner;

    public DtsDeliver(JobConfig jobConfig, List<Source> sourceList) {
        Destination destination = ReflectionUtils.newInstance("com.aliyun.dts.deliver.connector.desination.DStoreDestination");
        this.jobConfig = jobConfig;
        this.sourceList = sourceList;
        this.destination = destination;
    }

    public DtsDeliver(JobConfig jobConfig, List<Source> sourceList, Destination destination, Consumer<List<Pair<String, RecordCheckpoint>>> checkpointConsumer) {
        this.jobConfig = jobConfig;
        this.sourceList = sourceList;
        this.destination = destination;

        this.checkpointConsumer = checkpointConsumer;
    }

    public void startup() throws Exception {

        settings = jobConfig.getSettings();

        initializeContext();

        initMetrics();

        initializeStatusReporter();

        doCheck();

        doDeliver();
    }

    //todo(yanmen)
    private void initializeStatusReporter() {
    }

    //todo(yanmen)
    private void initMetrics() {
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.timeWindow(5, TimeUnit.SECONDS);
        metricConfig.samples(2);

        JmxReporter jmxReporter = new JmxReporter("any");

        // initialize any-all core metrics
        coreMetrics = new Metrics(metricConfig);
        coreMetrics.addReporter(jmxReporter);
    }

    public static void redirectStdOutStdErr() throws FileNotFoundException {
        PrintStream printstream = new PrintStream(new FileOutputStream("./dts-deliver.out"));
        System.setOut(printstream);
        System.setErr(printstream);
    }

    private void doCheck() {
    }

    private void doDeliver() {
        integrationRunner = new DtsIntegrationRunner(settings, coreMetrics, destination, sourceList,
                standaloneContext.getDefaultContext(), checkpointConsumer);

        integrationRunner.start();
    }

    private void initializeContext() {
        standaloneContext = new StandaloneContext(settings);
    }

    public void stop() {
        LOG.info("stop dts deliver...");

        integrationRunner.stop();

        coreMetrics.close();
    }
}
