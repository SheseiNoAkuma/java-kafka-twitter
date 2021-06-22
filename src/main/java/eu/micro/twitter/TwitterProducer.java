package eu.micro.twitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TwitterProducer {

    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    /**
     * <ul>
     *     <li>creat a twitter client</li>
     *     <li>create a kafka producer</li>
     *     <li>produce messages read from twitter</li>
     * </ul>
     */
    public static void main(String[] args) throws InterruptedException {
        logger.info("Twitter producer starting.....");

        var topic = "tweeter-topic";

        try (var connector = new TwitterConnector(List.of("kafka")); var producer = new Producer(topic)) {
            connector.poolMessage(producer);
        }

    }
}
