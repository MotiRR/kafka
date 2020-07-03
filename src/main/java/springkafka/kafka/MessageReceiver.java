package springkafka.kafka;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;


public class MessageReceiver {

    public static final Logger logger = LogManager.getLogger(MessageReceiver.class);

    private CountDownLatch latch = new CountDownLatch(1);

    private CountDownLatch partitionLatch = new CountDownLatch(2);

    public CountDownLatch getLatch() {
        return latch;
    }

    public CountDownLatch getPartitionLatch() {
        return partitionLatch;
    }

    @KafkaListener(topics = "${kafka.topic.name}", groupId = "${kafka.groupId}", containerFactory = "kafkaListenerContainerFactory")
    public void listen(String message) {
        logger.info(String.format("Received message %s", message));
        latch.countDown();
    }

    @KafkaListener(topics = "${kafka.topic.name}")
    public void listenWithHeaders(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        logger.info(String.format("(Partition) Received message: %s from partition %d", message, partition));
        partitionLatch.countDown();
    }

    @KafkaListener(groupId = "${partitioned.topic.name}", topicPartitions = {@TopicPartition(topic = "${kafka.topic.name}",
                    partitionOffsets = {
                            @PartitionOffset(partition = "${kafka.partition}", initialOffset = "${kafka.initialOffset}")})})
            //containerFactory = "kafkaListenerContainerFactory")
    public void listenToPartition(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        logger.info(String.format("(Offset) Received message: %s from partition %d", message, partition));
        partitionLatch.countDown();
    }

}