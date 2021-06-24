package eu.micro.elastic;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;

public class BlockingConsumer {

    private static final Logger logger = LoggerFactory.getLogger(BlockingConsumer.class);

    private final Properties properties;
    private final String topic;

    public BlockingConsumer(Properties properties, String topic) {
        this.properties = properties;
        this.topic = topic;
    }

    public void consume(Consumer<ConsumerRecords<String, String>> lambdaConsumer) {
        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe the consumer
        // note an alternative to subscribe is assign and seek where you select your topic / partition / topic
        consumer.subscribe(Collections.singleton(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            logger.info("read {} records", records.count());

            if (!records.isEmpty())
                lambdaConsumer.accept(records);

            logger.info("committing the offset...");
            consumer.commitSync();
        }
    }
}
