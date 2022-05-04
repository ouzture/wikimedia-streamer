package com.oakensoft.kafka.consumer;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;

public class ConsumerOpenSearch {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerOpenSearch.class);

    private static String indexName = "wikimedia";
    private static String topicName = "wikimedia.recentchange";

    public static void main(String[] args) throws IOException {
        logger.info("Starting consumer");

        try (RestHighLevelClient openSearchClient = new ClientOpensearch().createOpenSearchClient();
             KafkaConsumer<String, String> consumer = new ConsumerFactory().createConsumer()) {
            //create index if doesnt exists
            if (!openSearchClient.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT)) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);

                CreateIndexResponse response = openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                logger.info(response.toString());
                logger.info("Index wikimedia created");
            } else {
                logger.info("Index wikimedia exists");
            }

            consumer.subscribe(Collections.singleton(topicName));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records) {

                    String id = JsonParser.parseString(record.value())
                            .getAsJsonObject()
                            .get("meta")
                            .getAsJsonObject()
                            .get("id")
                            .getAsString();

                    IndexRequest indexRequest = new IndexRequest(indexName)
                            .source(record.value(), XContentType.JSON)
                            .id(id);

                    //id for idempotency, if no id provided in message
                    //String id = record.topic()+"_"+ record.partition()+"_"+ record.offset();

                    bulkRequest.add(indexRequest);

                }
                if (bulkRequest.numberOfActions()> 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    logger.info("Inserted {} records in {} seconds",bulkResponse.getItems().length,bulkResponse.getTook().getSeconds());
                }
                consumer.commitSync();
            }

        }


    }
}
