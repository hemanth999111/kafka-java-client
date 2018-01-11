import kafka.admin.AdminClient;
import kafka.coordinator.GroupOverview;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.immutable.List;
import scala.concurrent.JavaConversions;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ConsumerMetadataUsingAdminClient {

    private static String BOOTSTRAP_SERVERS = "slc11bmw.us.oracle.com:9092";
    private static AdminClient adminClient = createAdminClient(BOOTSTRAP_SERVERS);
    private static Consumer consumer = createConsumer(StringDeserializer.class.getName(), StringDeserializer.class
            .getName());
    //when ever scale out or scale in happens, both above variables needs to be updated.
    private static Map<String, Set<String>> topicConsumerListMap;
    private static Map<String, ConsumerGroupMetadata> consumerGroupMetadataMap;
    private static final boolean neededConsumerGroupMetadataCache = true;
    private static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(ConsumerMetadataUsingAdminClient.class);

    public static void main(String args[]) throws InterruptedException {
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleWithFixedDelay(new ConsumerMetadataUsingAdminClient().new ConsumerMetadataThread(), 0, 2,
                TimeUnit.SECONDS);
        while(topicConsumerListMap == null || consumerGroupMetadataMap == null) {
            LOG.info("sleeping");
            Thread.sleep(1000);
        }
        while(true) {
            printAllData();
            Thread.sleep(3000);
        }
        //scheduledExecutorService.shutdown();
    }

    private static void printAllData() {
        LOG.info("------------------------------------------------------------------------------------------");
        consumerGroupMetadataMap.forEach((key, value) -> {LOG.info("key is " + key + " value is " + value);});
        LOG.info("------------------------------------------------------------------------------------------");
        topicConsumerListMap.forEach((key, value) -> {LOG.info("key is " + key + " value is " + value);});
        LOG.info("------------------------------------------------------------------------------------------");
    }

    public static Map<String, Set<String>> getTopicConsumerListMap() {
        return topicConsumerListMap;
    }

    public static void setTopicConsumerListMap(Map<String, Set<String>> topicConsumerListMap) {
        ConsumerMetadataUsingAdminClient.topicConsumerListMap = topicConsumerListMap;
    }

    public static Map<String, ConsumerGroupMetadata> getConsumerGroupMetadataMap() {
        return consumerGroupMetadataMap;
    }

    public static void setConsumerGroupMetadataMap(Map<String, ConsumerGroupMetadata> consumerGroupMetadataMap) {
        ConsumerMetadataUsingAdminClient.consumerGroupMetadataMap = consumerGroupMetadataMap;
    }

    ConsumerMetadataUsingAdminClient() {
        topicConsumerListMap = new ConcurrentHashMap<>(300, 0.75f, 1);
    }

    private static AdminClient createAdminClient(String BOOTSTRAP_SERVERS) {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        return AdminClient.create(properties);
    }

    public static ConsumerGroupMetadata getConsumerGroupMetadata(String
                                                                         consumerGroupName) {
        AdminClient.ConsumerGroupSummary consumeGroupSummary = adminClient.describeConsumerGroup(consumerGroupName);
        ConsumerGroupMetadata consumerGroupMetadata = new ConsumerGroupMetadata();
        String state = consumeGroupSummary.state();
        Option<List<AdminClient.ConsumerSummary>> consumers = consumeGroupSummary.consumers();
        scala.collection.Map<TopicPartition, Object> offsets = adminClient.listGroupOffsets
                (consumerGroupName);
        //Map<TopicPartition, Object> offsetsMap = JavaConverters.mapAsJavaMap(offsets);

        consumerGroupMetadata.setState(state);
        if (state == "Dead") {
            return consumerGroupMetadata;
        }
        consumerGroupMetadata.setConsumerTopicPartitionMetadataList(new ArrayList<>());
        java.util.Set<TopicPartition> topicPartitionSet = new HashSet<>();

        Iterator<AdminClient.ConsumerSummary> consumersIterator = consumers.get().iterator();
        while (consumersIterator.hasNext()) {
            AdminClient.ConsumerSummary consumerSummary = consumersIterator.next();
            List<TopicPartition> assignedTopicPartitions = consumerSummary.assignment();
            Iterator<TopicPartition> assignedTopicPartitionsIterator = assignedTopicPartitions.iterator();
            while (assignedTopicPartitionsIterator.hasNext()) {
                TopicPartition topicPartition = assignedTopicPartitionsIterator.next();
                topicPartitionSet.add(topicPartition);
                //offsetsMap.remove(topicPartition);
                String topic = topicPartition.topic();
                int partition = topicPartition.partition();
                Long currentOffset = (Long) offsets.get(topicPartition).get();
                //Check if NoSuchElementException is thrown
                Long logEndOffset = getLogEndOffset(topicPartition);
                Long lag = getLag(currentOffset, logEndOffset);
                String consumerId = consumerSummary.consumerId();
                String host = consumerSummary.host();
                String clientId = consumerSummary.clientId();
                Long timeStamp = System.currentTimeMillis();
                ConsumerTopicPartitionMetadata consumerTopicPartitionMetadata = new ConsumerTopicPartitionMetadata
                        (topic, partition, currentOffset, logEndOffset, lag, consumerId, host, clientId, timeStamp);
                consumerGroupMetadata.getConsumerTopicPartitionMetadataList().add(consumerTopicPartitionMetadata);
            }
        }

        scala.collection.mutable.Set<TopicPartition> topicPartitionSetScala = JavaConverters.asScalaSet(topicPartitionSet);
        Iterator<Tuple2<TopicPartition, Object>> offsetsIterator = offsets.iterator();
        while(offsetsIterator.hasNext()) {
            Tuple2<TopicPartition, Object> element = offsetsIterator.next();
            TopicPartition topicPartition = element._1;
            if(!topicPartitionSetScala.contains(topicPartition)) {
                String topic = topicPartition.topic();
                int partition = topicPartition.partition();
                Long currentOffset = (Long) element._2;
                Long logEndOffset = getLogEndOffset(topicPartition);
                Long lag = getLag(currentOffset, logEndOffset);
                Long timeStamp = System.currentTimeMillis();
                ConsumerTopicPartitionMetadata consumerTopicPartitionMetadata = new ConsumerTopicPartitionMetadata(topic,
                        partition, currentOffset, logEndOffset, lag, null, null, null, timeStamp);
                consumerGroupMetadata.getConsumerTopicPartitionMetadataList().add(consumerTopicPartitionMetadata);
            }
        }
        /*for (Map.Entry<TopicPartition, Object> entry : offsetsMap.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            String topic = topicPartition.topic();
            int partition = topicPartition.partition();
            Long currentOffset = (Long) entry.getValue();
            Long logEndOffset = getLogEndOffset(topicPartition);
            Long lag = getLag(currentOffset, logEndOffset);
            Long timeStamp = System.currentTimeMillis();
            ConsumerTopicPartitionMetadata consumerTopicPartitionMetadata = new ConsumerTopicPartitionMetadata(topic,
                    partition, currentOffset, logEndOffset, lag, null, null, null, timeStamp);
            consumerGroupMetadata.getConsumerTopicPartitionMetadataList().add(consumerTopicPartitionMetadata);
        }*/
        return consumerGroupMetadata;
    }

    private static Long getLag(Long currentOffset, Long logEndOffset) {
        if (currentOffset != null && logEndOffset != null) {
            return logEndOffset - currentOffset;
        }
        return null;
    }

    private static Long getLogEndOffset(TopicPartition topicPartition) {
        consumer.assign(Arrays.asList(new TopicPartition[]{topicPartition}));
        consumer.seekToEnd(Arrays.asList(new TopicPartition[]{topicPartition}));
        return consumer.position(topicPartition);
    }

    private static <T, V> Consumer<T, V> createConsumer(String keyDeserializer, String valueDeserializer) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        final Consumer<T, V> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    public class ConsumerMetadataThread implements Runnable {

        /**
         * When an object implementing interface <code>Runnable</code> is used
         * to create a thread, starting the thread causes the object's
         * <code>run</code> method to be called in that separately executing
         * thread.
         * <p>
         * The general contract of the method <code>run</code> is that it may
         * take any action whatsoever.
         *
         * @see Thread#run()
         */
        @Override
        public void run() {
            Long startTime = System.currentTimeMillis();
            Map<String, ConsumerGroupMetadata> consumerGroupMetadataMapTemp = null;
            Map<String, Set<String>> topicConsumerListMapTemp = new HashMap<>();
            if (neededConsumerGroupMetadataCache) {
                consumerGroupMetadataMapTemp = new HashMap<>();
            }
            List<GroupOverview> consumerGroupList = adminClient
                    .listAllConsumerGroupsFlattened();
            scala.collection.Iterator<GroupOverview> consumerGroupIterator = consumerGroupList.iterator();
            while (consumerGroupIterator.hasNext()) {
                GroupOverview groupOverview = consumerGroupIterator.next();
                String consumerGroupName = groupOverview.groupId();
                ConsumerGroupMetadata consumerGroupMetadata = getConsumerGroupMetadata(consumerGroupName);
                if (neededConsumerGroupMetadataCache) {
                    consumerGroupMetadataMapTemp.put(consumerGroupName, consumerGroupMetadata);
                }

                for (ConsumerTopicPartitionMetadata consumerTopicPartitionMetadata : consumerGroupMetadata
                        .getConsumerTopicPartitionMetadataList()) {
                    String topic = consumerTopicPartitionMetadata.getTopic();
                    if (topicConsumerListMapTemp.containsKey(topic)) {
                        topicConsumerListMapTemp.get(topic).add(consumerGroupName);
                    } else {
                        topicConsumerListMapTemp.put(topic, new HashSet<String>());
                        topicConsumerListMapTemp.get(topic).add(consumerGroupName);
                    }
                }
            }
            if (neededConsumerGroupMetadataCache) {
                ConsumerMetadataUsingAdminClient.setConsumerGroupMetadataMap(consumerGroupMetadataMapTemp);
            }
            ConsumerMetadataUsingAdminClient.setTopicConsumerListMap(topicConsumerListMapTemp);
            Long endTime = System.currentTimeMillis();
            System.out.println("Time taken is " + (endTime - startTime));
        }
    }
}

class ConsumerGroupMetadata {
    private java.util.List<ConsumerTopicPartitionMetadata> consumerTopicPartitionMetadataList;
    private String state;

    public java.util.List<ConsumerTopicPartitionMetadata> getConsumerTopicPartitionMetadataList() {
        return consumerTopicPartitionMetadataList;
    }

    public void setConsumerTopicPartitionMetadataList(java.util.List<ConsumerTopicPartitionMetadata>
                                                              consumerTopicPartitionMetadataList) {
        this.consumerTopicPartitionMetadataList = consumerTopicPartitionMetadataList;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    @Override
    public String toString() {
        return "ConsumerGroupMetadata{" +
                "consumerTopicPartitionMetadataList=" + consumerTopicPartitionMetadataList +
                ", state='" + state + '\'' +
                '}';
    }
}

class ConsumerTopicPartitionMetadata {
    private String topic;
    private int parititon;
    private Long currentOffset;
    private Long logEndOffset;
    private Long lag;
    private String consumerId;
    private String host;
    private String clientId;
    private Long timeStamp;
    private String error;

    public ConsumerTopicPartitionMetadata(String topic, int parititon, Long currentOffset, Long logEndOffset, Long
            lag, String consumerId, String host, String clientId, Long timeStamp) {
        this.topic = topic;
        this.parititon = parititon;
        this.currentOffset = currentOffset;
        this.logEndOffset = logEndOffset;
        this.lag = lag;
        this.consumerId = consumerId;
        this.host = host;
        this.clientId = clientId;
        this.timeStamp = timeStamp;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getParititon() {
        return parititon;
    }

    public void setParititon(int parititon) {
        this.parititon = parititon;
    }

    public Long getCurrentOffset() {
        return currentOffset;
    }

    public void setCurrentOffset(Long currentOffset) {
        this.currentOffset = currentOffset;
    }

    public Long getLogEndOffset() {
        return logEndOffset;
    }

    public void setLogEndOffset(Long logEndOffset) {
        this.logEndOffset = logEndOffset;
    }

    public Long getLag() {
        return lag;
    }

    public void setLag(Long lag) {
        this.lag = lag;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    @Override
    public String toString() {
        return "ConsumerTopicPartitionMetadata{" +
                "topic='" + topic + '\'' +
                ", parititon=" + parititon +
                ", currentOffset=" + currentOffset +
                ", logEndOffset=" + logEndOffset +
                ", lag=" + lag +
                ", consumerId='" + consumerId + '\'' +
                ", host='" + host + '\'' +
                ", clientId='" + clientId + '\'' +
                ", timeStamp=" + timeStamp +
                ", error='" + error + '\'' +
                '}';
    }
}
