package eu.micro.elastic;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ElasticSearchConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    //NOTE: the default strategy is consume at least once, this mean I could receive multiple times the same message
    //for this reason is VERY IMPORTANT that my consumer is idempotent!!!!

    public static void main(String[] args) {
        logger.info("starting consumer");

        // create configuration
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "java-twitter-group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest - latest - none
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //disable autocommit and handle manually the offset commit
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20"); //max number of record I can receive with one pool

        var blockingConsumer = new BlockingConsumer(properties, "tweeter-topic");

        blockingConsumer.consume(new ElasticSearchClient());

    }
}
