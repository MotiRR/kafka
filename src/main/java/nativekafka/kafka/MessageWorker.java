package nativekafka.kafka;

import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import nativekafka.until.FileWorker;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MessageWorker {

    public static final Logger logger = LogManager.getLogger(MessageWorker.class);
    private FileWorker fileWorker = new FileWorker();

    /**
     * @param count количество отравляемых сообщений;
     * @param partition указывается в какой раздел будет производится запись сообщения;
     * @param topic тема сообщения;
     * */
    public void sendMessage(int count, int partition, String topic, String pathFileWithProperties) {
        Properties props = new Properties();
        fileWorker.getPropertiesFromFile(props, pathFileWithProperties);
        MessageSender ms = new MessageSender(props, topic);
        for (int i = 0; i < partition; i++) {
            for (int j = 0; j < count; j++)
                ms.sendMessage(i, "key", "value");
        }
    }

    private void createReceiver(List<MessageReceiver> list, int count, String offsetType, String pathFileWithProperties) {
        Properties props = new Properties();
        MessageReceiver messageReceiver;
        fileWorker.getPropertiesFromFile(props, pathFileWithProperties);
        messageReceiver = new MessageReceiver(props);
        if (count == 0) {
            messageReceiver.setTopics(List.of("stu"));
        } else {
            List<TopicPartition> ltp = new ArrayList<>(count);
            for (int i = 0; i < count; i++)
                ltp.add(new TopicPartition("stu", i));
            messageReceiver.setTopicPartition(ltp);
        }
        if(offsetType.equalsIgnoreCase("Beginning") || offsetType.equalsIgnoreCase("End"))
            messageReceiver.setOffset(offsetType, List.of(new TopicPartition("stu", 0)));
        if(offsetType.equalsIgnoreCase("Offset"))
            messageReceiver.setOffset(new TopicPartition("stu", 0), 2000);
        list.add(messageReceiver);
    }


    public void readMessage(int count, String offsetType, String pathFileWithProperties) {
        List<MessageReceiver> list = new ArrayList<>();
        createReceiver(list, count, offsetType, pathFileWithProperties);
        try {
            list.stream().forEach(MessageReceiver::start);
            Thread.sleep(500);
            for (MessageReceiver mr : list)
                mr.stop();
        } catch (InterruptedException ie) {
            logger.error(ie.getMessage());
        }
    }
}
