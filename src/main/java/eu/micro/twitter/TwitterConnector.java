package eu.micro.twitter;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class TwitterConnector implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(TwitterConnector.class);

    /*
     * Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
     */
    private final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

    private final Client hosebirdClient;

    public TwitterConnector(List<String> terms) {
        this.hosebirdClient = createClient(terms);
    }

    private Client createClient(List<String> terms) {

        // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        var hosebirdEndpoint = new StatusesFilterEndpoint();

        // this is for follow terms, to follow people use endpoint following
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1("zK9JsrSClYBcZaCxyQyvRGANV",
                "9VvQKJshgLPtYFEEUGL2rj26cqo7uvdsIH8ASeMrrPr6LMeZ17",
                "16599843-w627yFaGrj5NBX6w7pIcsDprNIJroUKWGMRfSXE6O",
                "0hb5Lk8SZCrb5q8MGMOymE27oQa17j9zSlMVJdBEi2ogU");

        ClientBuilder builder = new ClientBuilder()
                .name("Twitter-Client-01") // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    public void poolMessage(Consumer<String> consumer) throws InterruptedException {
        hosebirdClient.connect();
        while (!hosebirdClient.isDone()) {
            String pool = msgQueue.poll(5, TimeUnit.SECONDS);
            if (StringUtils.isNotBlank(pool))
                consumer.accept(pool);
        }
    }

    @Override
    public void close() {
        logger.info("close twitter connector");
        hosebirdClient.stop();
    }
}
