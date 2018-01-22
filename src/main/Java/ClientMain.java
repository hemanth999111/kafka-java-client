import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

//import kafka.admin.ConsumerGroupCommand;

import kafka.admin.AdminClient;
import kafka.admin.ConsumerGroupCommand;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.*;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.immutable.List;

public class ClientMain {

    private final static String TOPIC = "jaasacct-test";
    private final static String TOPIC1 = "jaasacct-test1";
    private final static String BOOTSTRAP_SERVERS = "slc11bmw.us.oracle.com:9092";
    //private final static String BOOTSTRAP_SERVERS = "10.252.136.161:6667";

    public static void main(String[] args) throws Exception {
        //getConsumerGroupDetailsUsingAdminClient(BOOTSTRAP_SERVERS, "console-consumer-73402");
        //getTopicsList();
        /*java.util.List<String> consumersList = getAllConsumerGroups(BOOTSTRAP_SERVERS);
        getConsumerGroupDetails(BOOTSTRAP_SERVERS, consumersList.get(0));*/
     /*if (args.length == 0) {
                runProducerAsynchronously(5);
		    } else {
		        runProducerAsynchronously(Integer.parseInt(args[0]));
		    }*/
        String topicName = "curdStream";//"compactTesting";
        System.out.println("Starting Producer");
        /*runProducerSynchronously(1, topicName, "Key35", "hello");
        runProducerSynchronously(1, topicName, "Key35", null);
        for(int i=36; i<50; i++) {
            runProducerSynchronously(1, topicName, "Key"+i, "kuchbhi");
        }*/
        runProducerSynchronously(5, topicName, "delete", "hello");
        runProducerSynchronously(5, topicName, "staying", "hello");
        runProducerSynchronously(1, topicName, "delete", null);
        System.out.println("Ended Producer\n\n\n");
        System.out.println("Starting Consumer\n");
        runConsumer(Arrays.asList(topicName), 20);
        System.out.println("\nEnded Consumer");
        //getTopicsList();
    }

    private static <T, V> Consumer<T, V> createConsumer(String keyDeserializer, String valueDeserializer, Collection<String> topics) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

//          props.put("enable.auto.commit", "true");
//          props.put("auto.commit.interval.ms", "1000");

        // Create the consumer using props.
        final Consumer<T, V> consumer =
                new KafkaConsumer<>(props);

        // Subscribe to the topic.
        //consumer.subscribe(Collections.singletonList(TOPIC));
        consumer.subscribe(topics);
        return consumer;
    }

    private static <T, V> void runConsumer(Collection<String> topics, int noOfIteration) {
        final Consumer<T, V> consumer = createConsumer(StringDeserializer.class.getName(), StringDeserializer.class.getName(), topics);

        //final int giveUp = 100;
        int noRecordsCount = 0;

        for(int i=0; i< noOfIteration; i++) {
            final ConsumerRecords<T, V> consumerRecords =
                    consumer.poll(1000);

            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                /*if (noRecordsCount > giveUp) break;
                else continue;*/
            }

            consumerRecords.forEach(record -> {
                //System.out.printf("Consumer Record: " + record.key() );//+" "+ record.value().toString() +" "+
                // String.valueOf(record.partition()) +" "+ String.valueOf(record.offset()));
                Date date = new Date(record.timestamp());
                TimeZone zone = TimeZone.getTimeZone("America/Los_Angeles"); // For example...
                DateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy"); // Put your pattern here
                format.setTimeZone(zone);
                String text = format.format(date);
                System.out.printf("Consumer Record:(key: %s, value: %s, partition: %d, offset: %d, timestamp: %s)\n", record.key(), record.value(), record
                        .partition(), record.offset(), text);
                /*if(record.value() == null) {
                    System.out.println("Heyy, I am not string null");
                } else if(record.value().equals("null")) {
                    System.out.println("Heyy, I am string null");
                }*/
            });

            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }

    private static <T, V> Producer<T, V> createProducer(String keySerializer, String valueSerializer) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        if (keySerializer == null) {
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    LongSerializer.class.getName());
        } else {
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    keySerializer);
        }
        if (valueSerializer == null) {
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class.getName());
        } else {
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    valueSerializer);
        }
        return new KafkaProducer<>(props);
    }

    public static void runProducerSynchronously(final int sendMessageCount, String topicName, String key, String message) throws Exception {
        final Producer<String, String> producer = createProducer(StringSerializer.class.getName(), StringSerializer.class.getName());
        long time = System.currentTimeMillis();

        try {
            for (long index = time; index < time + sendMessageCount; index++) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(topicName, key,
                                topicName + index); //record can be final

                if(message == null) {
                    record = new ProducerRecord<>(topicName, key,
                                    message);
                }

                RecordMetadata metadata = producer.send(record).get();

                long elapsedTime = System.currentTimeMillis() - time;
                System.out.printf("sent record(key=%s value=%s) " +
                                "meta(partition=%d, offset=%d) time=%d\n",
                        record.key(), record.value(), metadata.partition(),
                        metadata.offset(), elapsedTime);

                if(message==null) {
                    break;
                }

            }
        } finally {
            producer.flush();
            producer.close();
        }
    }

    static void runProducerAsynchronously(final int sendMessageCount) throws InterruptedException {
        final Producer<String, String> producer = createProducer(StringSerializer.class.getName(), StringSerializer
                .class.getName());
        long time = System.currentTimeMillis();
        final CountDownLatch countDownLatch = new CountDownLatch(sendMessageCount);

        try {
            for (long index = time; index < time + sendMessageCount; index++) {
                final ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC, TOPIC, TOPIC + index);
                producer.send(record, (metadata, exception) -> {
                    long elapsedTime = System.currentTimeMillis() - time;
                    if (metadata != null) {
                        System.out.printf("sent record(key=%s value=%s) " +
                                        "meta(partition=%d, offset=%d) time=%d\n",
                                record.key(), record.value(), metadata.partition(),
                                metadata.offset(), elapsedTime);
                    } else {
                        exception.printStackTrace();
                    }
                    countDownLatch.countDown();
                });
            }
            countDownLatch.await(25, TimeUnit.SECONDS);
        } finally {
            producer.flush();
            producer.close();
        }
    }


    public static void getTopicsList() {
        // TODO Auto-generated method stub
        Properties properties = new Properties();
        Map<String, java.util.List<PartitionInfo>> topics;
        properties.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        //10.252.136.85:6667
        properties.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        properties.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "true");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        topics = kafkaConsumer.listTopics();
        for (Entry<String, java.util.List<PartitionInfo>> entry : topics.entrySet()) {
            System.out.println(entry.getKey());
        }
    }

    public static void printAllConsumersUsingScalaClient(String BOOTSTRAP_SERVERS) {
        ConsumerGroupCommand.main(new String[]{"--list", "--bootstrap-server", BOOTSTRAP_SERVERS});
    }

    public static java.util.List<String> getAllConsumerGroups(String BOOTSTRAP_SERVERS) {
        ConsumerGroupCommand.ConsumerGroupCommandOptions opts = new ConsumerGroupCommand.ConsumerGroupCommandOptions
                (new String[]{"--list", "--bootstrap-server", BOOTSTRAP_SERVERS});
        ConsumerGroupCommand.KafkaConsumerGroupService consumerGroupService = new ConsumerGroupCommand
                .KafkaConsumerGroupService(opts);
        List<String> groupsListTemp = consumerGroupService.listGroups();
        java.util.List<String> groupsList = JavaConverters.seqAsJavaList(groupsListTemp);
        groupsList.stream().forEach(System.out::println);
        return groupsList;
        //System.out.println("List is " + groupsList.toString());
    }

    public static void getConsumerGroupDetails(String BOOTSTRAP_SERVERS, String consumerGroupName) {
        ConsumerGroupCommand.ConsumerGroupCommandOptions opts = new ConsumerGroupCommand.ConsumerGroupCommandOptions
                (new String[]{"--describe", "--group", consumerGroupName, "--bootstrap-server", BOOTSTRAP_SERVERS});
        ConsumerGroupCommand.KafkaConsumerGroupService consumerGroupService = new ConsumerGroupCommand
                .KafkaConsumerGroupService(opts);
        String name = consumerGroupService.describeGroup()._1.iterator().next().toString();
        //PartitionAssignmentState group = consumerGroupService.describeGroup()._2.iterator().next().iterator().next();
    }

    public static void getConsumerGroupDetailsUsingAdminClient(String BOOTSTRAP_SERVERS, String consumerGroupName) {

        AdminClient adminClient = createAdminClient(BOOTSTRAP_SERVERS);
        AdminClient.ConsumerGroupSummary consumeGroupSummary = adminClient.describeConsumerGroup(consumerGroupName);
        String state = consumeGroupSummary.state();
        Option<List<AdminClient.ConsumerSummary>> consumers = consumeGroupSummary.consumers();
        scala.collection.Map<TopicPartition, Object> offsets = adminClient.listGroupOffsets
                (consumerGroupName);
        java.util.Map<TopicPartition, Object> offsetsMap = JavaConverters.mapAsJavaMap(offsets);
        if (consumers.get() != null) {
            java.util.List<AdminClient.ConsumerSummary> consumersjava = JavaConverters.seqAsJavaList(consumers.get());
            consumersjava.stream().forEach((consumerSummary) -> {
                for (TopicPartition topicPartition : JavaConverters.seqAsJavaList(consumerSummary.assignment())) {
                    System.out.println(topicPartition.topic() + "    " + topicPartition.partition() + "    " +
                            offsetsMap.get
                            (topicPartition) + "    " + getLogEndOffset(topicPartition) + "    " + consumerSummary.consumerId() + "    " + consumerSummary.host
                            () + "    " + consumerSummary.clientId());
                }
                /*System.out.println(" Client Id is " + consumerSummary.clientId() + " and consumer Id is " +
                        consumerSummary.consumerId() + " and host is " + consumerSummary.host() + " topic parititons " +
                        "is " + (JavaConverters.seqAsJavaList(consumerSummary.assignment())).toString());*/
            });
        }
        /*if (offsets != null) {
            java.util.Map<TopicPartition, Object> offsetsMap = JavaConverters.mapAsJavaMap(offsets);
            for (Entry<TopicPartition, Object> entry : offsetsMap.entrySet()) {
                Long temp = (Long) entry.getValue();
                System.out.println("topic is " + entry.getKey().topic() + " partition is " + entry.getKey().partition
                        () + " and offset is " + temp);
            }
        }*/
    }

    private static Long getLogEndOffset(TopicPartition topicPartition) {
        Consumer<Object, Object> consumer = createConsumer(StringDeserializer.class.getName(), StringDeserializer.class.getName(), Arrays.asList(TOPIC, TOPIC1));
        consumer.assign(Arrays.asList(new TopicPartition[]{topicPartition}));
        consumer.seekToEnd(Arrays.asList(new TopicPartition[]{topicPartition}));
        return consumer.position(topicPartition);
    }

    private static AdminClient createAdminClient(String BOOTSTRAP_SERVERS) {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        return AdminClient.create(properties);
    }


    public static void describeConsumerGroup(String BOOTSTRAP_SERVERS) {
    }

    public static class ByteUtils {
        private static ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);

        public static byte[] longToBytes(long x) {
            buffer.putLong(0, x);
            return buffer.array();
        }

        public static long bytesToLong(byte[] bytes) {
            buffer.put(bytes, 0, bytes.length);
            buffer.flip();//need flip
            return buffer.getLong();
        }
    }

}

/*class PartitionAssignmentState {
    String group;
	Option[Node] coordinator;


	PartitionAssignmentState(group: String, coordinator: Option[Node], topic: Option[String],
							 partition: Option[Int], offset: Option[Long], lag: Option[Long],
							 consumerId: Option[String], host: Option[String],
							 clientId: Option[String], logEndOffset: Option[Long])
}*/
