package eu.micro.twitter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.function.Consumer;

public class Producer implements AutoCloseable, Consumer<String> {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;

    public Producer(String topic) {
        this.topic = topic;

        // create configuration
        var properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // idempotent producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // should also set min.insync.replicas=2 (or more) on topic or broker

        // improve the throughput
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // 'gzip', 'snappy', 'lz4', 'zstd'
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); // milliseconds to wait for batch together more messages
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // max size of a batch

        // create producer
        kafkaProducer = new KafkaProducer<>(properties);
    }

    @Override
    public void close() {
        logger.info("closing kafka producer");
        kafkaProducer.flush();
        kafkaProducer.close();
    }

    @Override
    public void accept(String s) {
        // send data -async
        ProducerRecord<String, String> data = new ProducerRecord<>(topic, s);
        logger.info("about to produce {}", data);
        kafkaProducer.send(data, (recordMetadata, e) -> {
            // execute every time record was successfully sent or an exception is thrown
            if (e != null)
                logger.error("some error occurred", e);
            else
                logger.info("Message sent with metadata: topic<{}>, partition<{}>, offset<{}>, timestamp<{}>",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
        });
    }
}
