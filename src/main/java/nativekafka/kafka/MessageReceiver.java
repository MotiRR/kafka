package nativekafka.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class MessageReceiver {

    public static final Logger logger = LogManager.getLogger(MessageReceiver.class);

    private KafkaConsumer<String, String> consumer;
    private final AtomicBoolean isInterrupted = new AtomicBoolean(false);
    private final Thread receiverThread;

    private long count = 0;

    public MessageReceiver(Properties kafkaProps) {
        this.consumer = new KafkaConsumer<String, String>(kafkaProps);
        receiverThread = new Thread(getReceiver());
    }

    public void setTopics(List<String> topics) {
        consumer.subscribe(topics);
    }

    public void setTopicPartition(List<TopicPartition> tp) {
        consumer.assign(tp);
    }

    public void setOffset(String seekType, List<TopicPartition> tp) {
        if (seekType.equalsIgnoreCase("Beginning"))
            consumer.seekToBeginning(tp);
        if (seekType.equalsIgnoreCase("End"))
            consumer.seekToEnd(tp);
    }

    public void setOffset(TopicPartition tp, long offset) {
        consumer.seek(tp, offset);
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
    private String convertMillInDate(long millis) {
        Date date = new Date(millis);
        SimpleDateFormat sdf = new SimpleDateFormat("EEEE,MMMM d,yyyy h:mm,a");
        //sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(date);
    }

    private Runnable getReceiver() {
        return () -> {
            while (!isInterrupted.get()) {
                try {

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info(String.format("received record: topic = %s, partition = %d, offset = %d, date = %s, " +
                                        "key = %s, value = %s", record.topic(), record.partition(), record.offset(), convertMillInDate(record.timestamp()),
                                record.key(), record.value()));
                        count++;
                    }
                } catch (Exception ex) {
                    logger.error("exception occurred: " + ex.getMessage());
                }
            }
        };
    }

}