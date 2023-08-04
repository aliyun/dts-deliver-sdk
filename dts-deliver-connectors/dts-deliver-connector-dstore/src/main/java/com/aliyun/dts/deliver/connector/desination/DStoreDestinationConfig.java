package com.aliyun.dts.deliver.connector.desination;

import com.aliyun.dts.deliver.commons.config.GlobalSettings;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.commons.json.Jsons;
import com.aliyun.dts.deliver.commons.openapi.DtsOpenApi;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.taobao.drc.togo.client.producer.TogoProducer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;

public class DStoreDestinationConfig {
    protected static final Logger LOGGER = LoggerFactory.getLogger(DStoreDestinationConfig.class);

    private TogoProducer producer;

    private Properties properties;

    public DStoreDestinationConfig(final TogoProducer producer, final Properties properties) {
        this.producer = producer;
        this.properties = properties;
    }

    public static DStoreDestinationConfig getDStoreDestinationConfig(Settings settings, Pair<String, String> userPassword, long sinkId) {

        Properties properties = getProducerProperties(settings, userPassword, sinkId);

        LOGGER.info("all dstore properties are: ");
        for (String key : properties.stringPropertyNames()) {
            LOGGER.info("\t{}: {}", key, properties.get(key));
        }

        return new DStoreDestinationConfig(new TogoProducer(properties), properties);
    }

    public static Properties getProducerProperties(Settings settings, Pair<String, String> userPassword, long sinkId) {
        Properties props = new Properties();

        initCommonClientProps(props);

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, GlobalSettings.DTS_BOOTSTRAP_SERVERS_CONFIG.getValue(settings));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, getClientId("producer", GlobalSettings.DTS_JOB_ID.getValue(settings), sinkId));
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 0); //重试0次，避免数据重复
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, DStoreReplicateHandler.KAFKA_BATCH_SIZE_CONFIG.getValue(settings));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, DStoreReplicateHandler.KAFKA_BUFFER_MEMORY_CONFIG.getValue(settings)); //
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MAX_VALUE);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, DStoreReplicateHandler.KAFKA_REQUEST_TIMEOUT_MS_CONFIG.getValue(settings));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, DStoreReplicateHandler.KAFKA_COMPRESSION_TYPE_CONFIG.getValue(settings));
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1024 * 1024 * 1024); // 1GB
        props.put(ProducerConfig.SEND_BUFFER_CONFIG,  DStoreReplicateHandler.KAFKA_BATCH_SIZE_CONFIG.getValue(settings) * 4); // 32MB
        props.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, DStoreReplicateHandler.KAFKA_BATCH_SIZE_CONFIG.getValue(settings) * 4); // 32MB
        props.put(ProducerConfig.LINGER_MS_CONFIG, DStoreReplicateHandler.LINGER_MS_CONFIG.getValue(settings));

        // enable leader switch interrupt.
        props.put("leaderSwitchInterrupt", "true");

        //protocol props
        props.put(SaslConfigs.SASL_JAAS_CONFIG, buildJaasConfig(userPassword.getLeft(), userPassword.getRight()));

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, GlobalSettings.KAFKA_SECURITY_PROTOCOL.getValue(settings));
        props.put("sasl.mechanism", GlobalSettings.KAFKA_SASL_MECHANISM.getValue(settings));

        props.put("timeout.ms", DStoreReplicateHandler.KAFKA_REQUEST_TIMEOUT_MS_CONFIG.getValue(settings));
        props.put("session.timeout.ms", DStoreReplicateHandler.KAFKA_REQUEST_TIMEOUT_MS_CONFIG.getValue(settings));

        // clientCoordinator relates config items
        props.put(DStoreReplicateHandler.CLIENT_COORDINATOR_POLL_TIMEOUT_MS_CONFIG.getKey(), DStoreReplicateHandler.CLIENT_COORDINATOR_POLL_TIMEOUT_MS_CONFIG.getValue(settings));
        props.put(DStoreReplicateHandler.CLIENT_COORDINATOR_REBALANCE_TIMEOUT_MS_CONFIG.getKey(), DStoreReplicateHandler.CLIENT_COORDINATOR_REBALANCE_TIMEOUT_MS_CONFIG.getValue(settings));
        props.put(DStoreReplicateHandler.CLIENT_COORDINATOR_SESSION_TIMEOUT_MS_CONFIG.getKey(), DStoreReplicateHandler.CLIENT_COORDINATOR_SESSION_TIMEOUT_MS_CONFIG.getValue(settings));
        props.put(DStoreReplicateHandler.CLIENT_COORDINATOR_HEARTBEAT_INTERVAL_MS_CONFIG.getKey(), DStoreReplicateHandler.CLIENT_COORDINATOR_HEARTBEAT_INTERVAL_MS_CONFIG.getValue(settings));

        //load all kafka properties
        settings.getSettings().forEach((key, v) -> {
            if (key.startsWith("dstore.")) {
                String toPutKey = key.substring(7);
                props.setProperty(toPutKey, (String) v);
            }
        });

        return props;
    }

    private static void initCommonClientProps(Properties props) {
        props.put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, 100L);
        props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 5 * 60 * 1000);
        props.put(CommonClientConfigs.SEND_BUFFER_CONFIG, 128 * 1024);
        props.put(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG, 100L);
        props.put(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000L);
        props.put(CommonClientConfigs.RECEIVE_BUFFER_CONFIG, 32 * 1024);
        props.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, 40 * 1000);
    }

    private static String getClientId(String role, String jobId, long sinkId) {
        String localIp = "";
        try {
            localIp = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
        }
        return String.format("%s-%s-%s-%s", role, jobId, localIp, sinkId);
    }


    private static Map<?,?> propertiesByProtocol(JsonNode config) {
        final JsonNode protocolConfig = config.get("protocol");
        LOGGER.info("Kafka protocol config: {}", protocolConfig.toString());
        final KafkaProtocol protocol = KafkaProtocol.valueOf(protocolConfig.get("security_protocol").asText().toUpperCase());
        final ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
                .put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol.toString());

        switch (protocol) {
            case PLAINTEXT :
                break;
            case SASL_SSL:
            case SASL_PLAINTEXT :
                builder.put(SaslConfigs.SASL_JAAS_CONFIG, buildJaasConfig(config.get("dts_user").asText(), config.get("dts_password").asText()));
                builder.put(SaslConfigs.SASL_MECHANISM, protocolConfig.get("sasl_mechanism").asText());
                break;
            default :
                throw new RuntimeException("Unexpected Kafka protocol: " + Jsons.serialize(protocol));
        }

        return builder.build();
    }

    private static Object buildJaasConfig(String user, String password) {
        String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";

        return String.format(jaasTemplate, user,  password);
    }

    public TogoProducer getProducer() {
        return producer;
    }

    public Properties getProperties() {
        return properties;
    }

}
