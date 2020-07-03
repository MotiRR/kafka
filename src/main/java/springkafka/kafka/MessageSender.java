package springkafka.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Properties;

//@Service
public class MessageSender {

    public static final Logger logger = LogManager.getLogger(MessageSender.class);
    @Value(value = "${kafka.topic.name}")
    private String messageTopic;

    @Value(value = "${partitioned.topic.name}")
    private String partitionTopicName;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    public void sendMessage(String msg) {
        kafkaTemplate.send(messageTopic, msg);
    }

    public void sendMessageToPartition(int partition, String key, String message) {
        kafkaTemplate.send(partitionTopicName, partition, key, message);
    }

    public void sendMessageWithCallback(String msg) {

        ListenableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(messageTopic, msg);

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                logger.info(String.format("Sent message = %s, current offset = %d, topic = %s and " +
                                "partition = %d", msg, result.getRecordMetadata().offset(),
                        result.getRecordMetadata().topic(), result.getRecordMetadata().partition()));
            }
            @Override
            public void onFailure(Throwable ex) {
                logger.warn(String.format("Unable to send message = %s, due to : %s", msg, ex.getMessage()));
            }
        });
    }
}
