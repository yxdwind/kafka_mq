package gold.jiaxin.transactional;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Iterator;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * TODO
 *
 * @author yanxiaodong
 * @version 1.0
 * @date 2020/10/23 21:31
 */
public class KafkaConsumerUncommited {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOSA:9092,CentOSB:9092,CentOSC:9092,");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");

        prop.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommited");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

        //订阅相关的topic
        consumer.subscribe(Pattern.compile("^topic.*"));

        //遍历消息队列
        for (; ; ) {
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(1));

            if (!poll.isEmpty()) {
                Iterator<ConsumerRecord<String, String>> iterator = poll.iterator();
                while (iterator.hasNext()) {
                    //获取一个消费消息
                    ConsumerRecord<String, String> record = iterator.next();
                    String topic = record.topic();
                    int partition = record.partition();
                    long offset = record.offset();
                    String key = record.key();
                    String value = record.value();
                    long timestamp = record.timestamp();

                    System.out.println(topic + "\t" + partition + "\t" + offset + "\t" + key + "\t" + value);

                }
            }
        }
    }
}
