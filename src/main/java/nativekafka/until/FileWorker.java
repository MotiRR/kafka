package nativekafka.until;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class FileWorker {

    public static final Logger logger = LogManager.getLogger(FileWorker.class);

    public void getPropertiesFromFile(Properties props, String fileName) {
        try(InputStream in = new FileInputStream(fileName)) {
            logger.info(String.format("Производится чтение property из файла %s", fileName));
            props.load(in);
        } catch (Exception e) {
            logger.warn(e.getMessage());
        }
    }
}
