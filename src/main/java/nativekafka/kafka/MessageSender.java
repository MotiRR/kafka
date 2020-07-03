package nativekafka.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Properties;

public class MessageSender {

    public static final Logger logger = LogManager.getLogger(MessageSender.class);

    private Producer<String, String> producer;
    private String messageTopic;

    public MessageSender(Properties kafkaProps, String messageTopic) {
        this.producer = new KafkaProducer<String, String>(kafkaProps);
        this.messageTopic = messageTopic;
    }

    public void sendMessage(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(messageTopic, key, value);
        try {
            producer.send(record);
        } catch (Exception e) {
            logger.warn(String.format("Unable to send message = %s, due to : %s", value, e.getMessage()));
        }
    }

    public void sendMessage(int partition, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(messageTopic, partition, key, value);
        try {
            producer.send(record);
        } catch (Exception e) {
            logger.warn(String.format("Unable to send message = %s, due to : %s", value, e.getMessage()));
        }
    }

    public void sendMessageWithCallback(int partition, String key, String value) {

        class EventProducer implements Callback {
            @Override
            public void onCompletion(RecordMetadata rm, Exception e) {
                if (e != null) {
                    logger.warn(String.format("Unable to send message = %s, due to : %s", value, e.getMessage()));
                }
                logger.info(String.format("Sent message = %s, current offset = %d, topic = %s and " +
                                "partition = %d", value, rm.offset(),
                        rm.topic(), rm.partition()));
            }
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(messageTopic, partition, key, value);
        try {

            producer.send(record, new EventProducer());
            /*RecordMetadata rm = producer.send(record).get();
            logger.info(String.format("Sent message = %s, current offset = %d, topic = %s and " +
                                "partition = %d", value, rm.offset(),
                        rm.topic(), rm.partition()));*/
        } catch (Exception e) {
            logger.warn(String.format("Unable to send message = %s, due to : %s", value, e.getMessage()));
        }

    }


}
