import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.common.TopicAndPartition;
import kafka.controller.ControllerContext;
import kafka.controller.KafkaController;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.ZkUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Time;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.Set;
import scala.concurrent.JavaConversions;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import static java.lang.Thread.sleep;

public class listTopicMetadata {

    //private final static String BOOTSTRAP_SERVERS = "slc11bmw.us.oracle.com:9092";
    private final static String BOOTSTRAP_SERVERS = "10.252.137.18:6667";
    private final static String ZOOKEEPER_SERVERS = "slc11bmw.us.oracle.com:2181";
    private final static ZkUtils zkUtils = ZkUtils.apply(ZOOKEEPER_SERVERS, 30000, 30000, false);
    private final static ControllerContext controllerContext = new ControllerContext(zkUtils);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    private static final CuratorFramework client = CuratorFrameworkFactory.newClient(ZOOKEEPER_SERVERS, retryPolicy);

    public static void main(String args[]) throws Exception {
        client.start();
        TreeCache treeCache = new TreeCache(client, "/brokers/topics");
        treeCache.start();
        String topicName = "jaasacct-hemanthtopic";
        while (true) {
            long startTime = System.currentTimeMillis();
            //printDataUsingKafkaConsumer();
            //printDataUsingZkUtils();
            //printDataUsingZkUtils(topicName);
            //printDataUsingApacheCurator(treeCache);
         long endTime = System.currentTimeMillis();
            System.out.println("Time taken is " + (endTime - startTime));
            sleep(5000);
        }
    }

    private static void printDataUsingApacheCurator(TreeCache treeCache) throws InterruptedException, IOException {
       /* treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {

            }
        });*/
        Map<String, ChildData> topicsMap = treeCache.getCurrentChildren("/brokers/topics");
        if (topicsMap == null) {
            System.out.println("not getting intialized");
            //System.exit(1);
            sleep(1000);
            return;
        }
        TopicDataInZookeeper topicDataInZookeeper = null;
        for (Map.Entry<String, ChildData> entry : topicsMap.entrySet()) {
            String topicName = entry.getKey();
            byte[] value = entry.getValue().getData();
            topicDataInZookeeper = objectMapper.readValue(new String(value), TopicDataInZookeeper.class);
            System.out.println("Number of partitions for topic " + topicName + " is " + topicDataInZookeeper.getPartitions().size());
        }
        System.out.println("Replication Factor is " + topicDataInZookeeper.getPartitions().entrySet().iterator().next().getValue().size());

    }

    private static void printDataUsingZkUtils() {
        Seq<String> topicsSeq = zkUtils.getAllTopics();
        scala.collection.mutable.Map<String, scala.collection.Map<Object, Seq<Object>>> topicPartitions = zkUtils
                .getPartitionAssignmentForTopics(topicsSeq);
        for (String topicName : JavaConverters.seqAsJavaList(topicsSeq)) {
            scala.collection.Map<Object, Seq<Object>> topicPartition = topicPartitions.get(topicName).get();
            System.out.println("Number of partitions for topic " + topicName + " is " + topicPartition.size());
        }
    }

    private static void printDataUsingZkUtils(String topicName) {
        Seq<String> topicsSeq = JavaConverters.asScalaBuffer(Arrays.asList(topicName));//zkUtils.getAllTopics();
        scala.collection.mutable.Map<String, scala.collection.Map<Object, Seq<Object>>> topicPartitions = zkUtils
                .getPartitionAssignmentForTopics(topicsSeq);
            scala.collection.Map<Object, Seq<Object>> topicPartition = topicPartitions.get(topicName).get();
            System.out.println("Number of partitions for topic " + topicName + " is " + topicPartition.size());
    }

    private static void printDataUsingKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "HemanthRocks");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization" +
                ".StringDeserializer");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization" +
                ".StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        Map<String, List<PartitionInfo>> topicsMap = consumer.listTopics();
        Iterator<Map.Entry<String, List<PartitionInfo>>> iterator = topicsMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, List<PartitionInfo>> entry = iterator.next();
            printTopicInfo(entry.getKey(), entry.getValue());
        }
    }

    public static void printTopicInfo(String topicName, KafkaConsumer<String, String> consumer) {
        List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topicName);
        printTopicInfo(topicName, partitionInfoList);
    }

    public static void printTopicInfo(String topicName, List<PartitionInfo> partitionInfoList) {
        int partitionsCount = partitionInfoList.size();
        System.out.println("Number of Partitions for " + topicName + " are " + partitionsCount);
        int replicasCount = 0;
        for (PartitionInfo partitionInfo : partitionInfoList) {
            replicasCount += partitionInfo.replicas().length;
        }
        System.out.println("Replicas Count for " + topicName + " is " + replicasCount);
        //System.out.println("Replication Factor for " + topicName + " is " + replicasCount/partitionsCount);
        System.out.println("-----------------------------------------------------------------------------");
    }


  /*  Properties brokerProps = new Properties();
        brokerProps.put("enable.zookeeper","true");
        brokerProps.put("zk.connect", "10.252.139.126:2181");
        brokerProps.put("port","9092");

    KafkaConfig kafkaConfig = new KafkaConfig(brokerProps);
    KafkaController kafkaController = new KafkaController(kafkaConfig, zkUtils, null, null, null, null);
    ControllerContext controllerContext1 = kafkaController.controllerContext();*/


   /* Set<String> topicsList = controllerContext1.allTopics();
            for(String topicName: JavaConverters.setAsJavaSet(topicsList)) {
        Set<TopicAndPartition> partitionsForTopic = controllerContext1.partitionsForTopic(topicName);
        int partitionsCount = partitionsForTopic.size();
        System.out.println("Number of partitions for topic " + topicName + " is " + partitionsCount);
    }*/

}

class TopicDataInZookeeper {
    String version;
    Map<String, List<Integer>> partitions;

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Map<String, List<Integer>> getPartitions() {
        return partitions;
    }

    public void setPartitions(Map<String, List<Integer>> partitions) {
        this.partitions = partitions;
    }
}
