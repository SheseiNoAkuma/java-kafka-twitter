package eu.micro.elastic;


import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Consumer;

public class ElasticSearchClient implements Consumer<ConsumerRecords<String, String>> {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchClient.class);

    private final RestHighLevelClient client;

    public ElasticSearchClient() {
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));
    }

    @Override
    public void accept(ConsumerRecords<String, String> tweets) {

        var bulkRequest = new BulkRequest();

        for(var tweet: tweets) {
            var request = new IndexRequest(tweet.topic());

            //I need an id to make elastic idempotent in index creation/update:

            //first option: use tweet.key if available (not in this case)
            //second: use the concatenation of topic + partition + offset witch is of course unique
            //third: obtain an id from message payload (in this case tweet id)

            //this way the consumer is idempotent
            request.id(extractId(tweet.value()));

            request.source(tweet.value(), XContentType.JSON);
            bulkRequest.add(request);
        }


        try {
            var bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            logger.info("item added with success {}", bulkResponse);
        } catch (IOException e) {
            logger.error("something went wrong", e);
        }
    }

    private String extractId(String tweet) {
        return JsonParser.parseString(tweet)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}
