package gold.jiaxin.dml;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * 必须将CentOSA,CentOSB,CentOSC三个主机的IP和主机名映射配置在操作系统
 *
 * @author yanxiaodong
 * @version 1.0
 * @date 2020/10/23 20:00
 */
public class KafkaTopicDml {
    public static void main(String[] args) {
        //1、创建kafkaAdminClient
        Properties prop = new Properties();
        prop.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOSA:9092,CentOSB:9092,CentOSC:9092,");
        KafkaAdminClient adminClient = (KafkaAdminClient) KafkaAdminClient.create(prop);

        //创建Topic信息
        adminClient.createTopics(Arrays.asList(new NewTopic("topic01", 3, (short) 3)));

        ListTopicsResult topicsResult = adminClient.listTopics();
        try {
            Set<String> names = topicsResult.names().get();
            for (String name : names) {
                System.out.println(name);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        adminClient.close();

    }
}
