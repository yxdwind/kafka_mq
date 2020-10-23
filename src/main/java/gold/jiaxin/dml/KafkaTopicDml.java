package gold.jiaxin.dml;

import org.apache.kafka.clients.admin.*;

import java.util.Arrays;
import java.util.Map;
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
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1、创建kafkaAdminClient
        Properties prop = new Properties();
        prop.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOSA:9092,CentOSB:9092,CentOSC:9092,");
        KafkaAdminClient adminClient = (KafkaAdminClient) KafkaAdminClient.create(prop);

        //创建Topic信息
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(new NewTopic("topic01", 3, (short) 3)));
        //同步创建
        createTopicsResult.all().get();

        ListTopicsResult topicsResult = adminClient.listTopics();
        Set<String> names = topicsResult.names().get();
        for (String name : names) {
            System.out.println(name);
        }

        //同步删除
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList("topic01", "topic02"));
        deleteTopicsResult.all().get();

        //查看topic详细信息
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList("topic01"));
        Map<String, TopicDescription> topicDescriptionMap = deleteTopicsResult.all().get();
        for (Map.Entry<String, TopicDescription> entry: topicDescriptionMap.entrySet()){
            System.out.println(entry.getKey()+"\t"+entry.getValue());
        }

        adminClient.close();

    }
}
