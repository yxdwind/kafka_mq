package gold.jiaxin.interceptor;

import gold.jiaxin.serializer.User;
import gold.jiaxin.serializer.UserDefineDeserializer;
import gold.jiaxin.serializer.UserDefineSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;

/**
 * TODO
 *
 * @author yanxiaodong
 * @version 1.0
 * @date 2020/10/23 21:03
 */
public class KafkaProducerInterceptor {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOSA:9092,CentOSB:9092,CentOSC:9092,");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, UserDefineSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserDefineDeserializer.class.getName());
        prop.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,KafkaProducerInterceptor.class);

        KafkaProducer<String, User> producer = new KafkaProducer(prop);
        User user;
        for (int i = 0; i < 10; i++) {
            user = new User(i, "user" + i, new Date());
            ProducerRecord<String, User> record = new ProducerRecord("topic01", 1, "key" + i, user);
            //发送消息给服务器
            producer.send(record);
        }

        producer.close();
    }
}
