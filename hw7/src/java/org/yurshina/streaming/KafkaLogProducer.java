package org.yurshina.streaming;

import org.apache.commons.io.IOUtils;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;

/**
 * @author Anastasiia_Iurshina
 */
public class KafkaLogProducer {

    public static void main(String... args) throws IOException, InterruptedException {
        Properties props = new Properties();
        props.put("metadata.broker.list", "10.0.2.15:6667,127.0.0.1:6667,sandbox.hortonworks.com:6667");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        String topic = "logs_topic";
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        List<String> logs = IOUtils.readLines(
                KafkaLogProducer.class.getResourceAsStream("imp.20131019.csv"),
                Charset.forName("UTF-8")
        );

        for (String line : logs) {
//            System.out.println(line);
            producer.send(new KeyedMessage<String, String>(topic, line));
            Thread.sleep(10L);
        }
        producer.close();
    }
}
