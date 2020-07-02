package kafka;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import until.FileWorker;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MessageWorker {

    public static final Logger logger = LogManager.getLogger(MessageWorker.class);
    private FileWorker fileWorker = new FileWorker();

    public void sendMessage(int count, int partition) {
        Properties props = new Properties();
        fileWorker.getPropertiesFromFile(props, "src\\main\\resources\\config\\producer\\1.properties");
        MessageSender ms = new MessageSender(props, "stu");
        for (int i = 0; i < partition; i++) {
            for (int j = 0; j < count; j++)
                ms.send(i, "key", "value");
        }
    }

    private void createReceiver(List<MessageReceiver> list, int count, boolean partition) {
        Properties props = new Properties();
        fileWorker.getPropertiesFromFile(props, "src\\main\\resources\\config\\consumer\\1.properties");
        for (int i = 0; i < count; i++)
            if (partition)
                list.add(new MessageReceiver(props, "stu", i));
            else
                list.add(new MessageReceiver(props, List.of("stu")));
    }

    public void readMessage(int count, boolean partition) {
        List<MessageReceiver> list = new ArrayList<>();
        createReceiver(list, count, partition);
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
