package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class MessageReceiver {

    public static final Logger logger = LogManager.getLogger(MessageReceiver.class);

    private KafkaConsumer<String, String> consumer;
    private final AtomicBoolean isInterrupted = new AtomicBoolean(false);
    private final Thread receiverThread;

    int count = 0;

    public MessageReceiver(Properties kafkaProps, List<String> topics) {
        this.consumer = new KafkaConsumer<String, String>(kafkaProps);
        consumer.subscribe(topics);
        receiverThread = new Thread(getReceiver());
    }

    public MessageReceiver(Properties kafkaProps, String topic, int partition) {
        this.consumer = new KafkaConsumer<String, String>(kafkaProps);
        consumer.assign(List.of(new TopicPartition(topic, partition)));
        receiverThread = new Thread(getReceiver());
    }

    public synchronized void start() {
        if (isInterrupted.get()) {
            throw new IllegalStateException("Method MessageSender::start used after receiver was interrupted");
        }
        logger.info("MessageSender started successfully");
        receiverThread.start();
    }

    public synchronized void stop() throws InterruptedException {
        if (isInterrupted.get()) {
            throw new IllegalStateException("Method MessageReceiver::stop used after receiver was interrupted");
        }

        isInterrupted.set(true);

        receiverThread.join();
        consumer.close();

        logger.info("MessageReceiver finished successfully");
        System.out.println(count);
    }

    private Runnable getReceiver() {
        return () -> {
            while (!isInterrupted.get()) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        String valueStr = record.value();
                        logger.info("received record: " + valueStr);
                        count++;
                    }
                } catch (Exception ex) {
                    logger.error("exception occurred: " + ex.getMessage());
                }
            }
        };
    }

}