package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
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

    public void send(int partition, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(messageTopic, partition, key, value);
        try {
            producer.send(record);
        } catch (Exception e) {
            logger.info(e.getMessage());
            logger.error(e.getMessage());
        }
    }
}
