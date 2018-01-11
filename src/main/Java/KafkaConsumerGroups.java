import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.*;
import org.apache.log4j.Logger;
//import org.apache.commons.lang.ArrayUtils;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.Thread.sleep;
import static org.apache.kafka.common.protocol.types.Type.*;

/**
 * Created by mohemant on 8/25/2017.
 */
public class KafkaConsumerGroups {

    private final static String CONSUMER_OFFSETS = "__consumer_offsets";
    private final static String TOPIC = "jaasacct-helooasdf";
    private final static String TOPIC1 = "jaasacct-test";
    private final static String TOPIC2 = "WordsWithCountsTopic";
    private final static String BOOTSTRAP_SERVERS = "slc11bmw.us.oracle.com:9092";
    //private final static String BOOTSTRAP_SERVERS = "10.252.137.86:6667";
    private static Map<GroupTopicPartition, OffsetAndMetadata> groupTopicPartitionOffsetAndMetadataMap = new
            ConcurrentHashMap<>();
    private static Map<GroupTopicPartition, MemberMetadata> groupTopicPartitionMemberMetadataMap = new
            ConcurrentHashMap<>();
    private static Map<String, Map<String, Map<Integer, Object>>> consumerTopicPartitionMap = new ConcurrentHashMap<>();
    private static Map<String, Map<String, Map<Integer, Object>>> topicConsumerPartitionMap = new ConcurrentHashMap<>();
    private final static AvroTest avroTest = new AvroTest();
    private final static String keySchemaInJSON = "{\"type\" : \"string\"}";
    private final static String valueSchemaInJSON = "{\"type\" : \"map\", \"values\" : {\"type\":\"map\", " +
            "\"values\": \"null\"}}";
    private static final Logger LOG = Logger.getLogger(KafkaConsumerGroups.class);

    private static boolean isTesting;

    //private static Map<String, List<String>> consumerTopicsListMap = new ConcurrentHashMap<>();
    //private static Map<String, List<String>> topicConsumersListMap = new ConcurrentHashMap<>();

    private final Schema OFFSET_COMMIT_KEY_SCHEMA = new Schema(new Field("group", STRING),
            new Field("topic", STRING),
            new Field("partition", INT32));

    private final Schema GROUP_METADATA_KEY_SCHEMA = new Schema(new Field("group", STRING));

    private final Schema OFFSET_COMMIT_VALUE_SCHEMA_V1 = new Schema(new Field("offset", INT64),
            new Field("metadata", STRING, "Associated metadata.", ""),
            new Field("commit_timestamp", INT64),
            new Field("expire_timestamp", INT64));

    private final Schema OFFSET_COMMIT_VALUE_SCHEMA_V0 = new Schema(new Field("offset", INT64),
            new Field("metadata", STRING, "Associated metadata.", ""),
            new Field("timestamp", INT64));

    private final Schema MEMBER_METADATA_V0 = new Schema(
            new Field("member_id", STRING),
            new Field("client_id", STRING),
            new Field("client_host", STRING),
            new Field("session_timeout", INT32),
            new Field("subscription", BYTES),
            new Field("assignment", BYTES));

    private final Schema MEMBER_METADATA_V1 = new Schema(
            new Field("member_id", STRING),
            new Field("client_id", STRING),
            new Field("client_host", STRING),
            new Field("session_timeout", INT32),
            new Field("rebalance_timeout", INT32),
            new Field("subscription", BYTES),
            new Field("assignment", BYTES));

    private final Schema GROUP_METADATA_VALUE_SCHEMA_V0 = new Schema(
            new Field("protocol_type", STRING),
            new Field("generation", INT32),
            new Field("protocol", STRING),
            new Field("leader", STRING),
            new Field("members", new ArrayOf(MEMBER_METADATA_V0)));

    private final Schema GROUP_METADATA_VALUE_SCHEMA_V1 = new Schema(
            new Field("protocol_type", STRING),
            new Field("generation", INT32),
            new Field("protocol", NULLABLE_STRING),
            new Field("leader", NULLABLE_STRING),
            new Field("members", new ArrayOf(MEMBER_METADATA_V1)));


    public static void main(String args[]) throws InterruptedException {
        new KafkaConsumerGroups().new GetConsumerDetailsUsingStreams().intializeAndStartStreamingData();
        /*TestConsume("aggregate-application-id-dummyStore-changelog", StringDeserializer.class.getName(),
                StringDeserializer.class.getName());*/
       /* Thread thread = new Thread(new ConsumerThread());
        thread.start();
        while(true) {
            sleep(10000);
            if(topicConsumerPartitionMap.containsKey(TOPIC1)) {
                System.out.println("Consumers for topic " + TOPIC1 + " are " + topicConsumerPartitionMap.get(TOPIC1)
                .keySet());
            }
        }*/
    }

    private static <K, V> void TestConsume(String topicName, String keyDeserializer, String valueDeserializer) {
        Consumer<K, V> consumer = createConsumer("dummy-test35" + UUID.randomUUID().toString(), topicName,
                keyDeserializer, valueDeserializer);
        //TopicPartition topicPartition = new TopicPartition("application-id1-Counts-changelog", 0);
        //consumer.assign(Arrays.asList(topicPartition));
        //consumer.seek(topicPartition, 1);
        System.out.println("starting polling");
        while (true) {
            ConsumerRecords<K, V> consumerRecords = consumer.poll(10000);
            System.out.println("completed polling");
            Iterator<ConsumerRecord<K, V>> iterator =
                    consumerRecords.iterator();
            while (iterator.hasNext()) {
                try {
                    ConsumerRecord<K, V> record = iterator.next();
                    /*byte[] array = record.key();
                    ByteBuffer byteBufferKey = ByteBuffer.wrap(array);
                    String key = byteBufferKey.toString();
                    byte[] array1 = record.key();
                    ByteBuffer byteBufferKey1 = ByteBuffer.wrap(array1);
                    Long value = byteBufferKey1.getLong();
                    System.out.println(record.toString() + " and key is " + key + " and value is " + value);*/
                    System.out.println("key is " + record.key().toString() + " and value is " + record.value()
                            .toString());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            /*Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
            map.put(topicPartition, new OffsetAndMetadata(3, "NO_METADATA"));
            map.put(topicPartition, new OffsetAndMetadata(4, "NO_METADATA"));*/
            consumer.commitSync();
        }

        //consumer.commitSync();
    }

    private static <T, V> Consumer<T, V> createConsumer(String groupName, String topicName, String keyDeserializer,
                                                        String valueDeSerializer) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                groupName);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//make it latest to avoid errors probably.
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false");
        if (keyDeserializer == null) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        } else {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        }
        if (valueDeSerializer == null) {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        } else {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeSerializer);
        }

//          props.put("enable.auto.commit", "true");
//          props.put("auto.commit.interval.ms", "1000");

        // Create the consumer using props.
        final Consumer<T, V> consumer =
                new KafkaConsumer<>(props);

        // Subscribe to the topic.
        if (topicName != null) {
            consumer.subscribe(Collections.singletonList(topicName));
        }
        //consumer.subscribe(Arrays.asList(TOPIC, TOPIC1));
        return consumer;
    }

    private class ConsumerThread implements Runnable {


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
            Consumer<byte[], byte[]> consumer = createConsumer(
                    "ConsumerGroupConsumer" + UUID.randomUUID().toString(), CONSUMER_OFFSETS, null, null);
            while (true) {
                System.out.println("starting polling");
                final ConsumerRecords<byte[], byte[]> consumerRecords =
                        consumer.poll(10000);
                Iterator<ConsumerRecord<byte[], byte[]>> iterator =
                        consumerRecords.iterator();
                System.out.println("completed polling");
                while (iterator.hasNext()) {
                    try {
                        ConsumerRecord<byte[], byte[]> record = iterator.next();

                        byte[] array = record.key();
                        ByteBuffer byteBuffer = ByteBuffer.wrap(array);
                        Short version = byteBuffer.getShort();
                        if (version == 2) {
                            Struct key = GROUP_METADATA_KEY_SCHEMA.read(byteBuffer);
                            String group = key.get(GROUP_METADATA_KEY_SCHEMA.get("group")).toString();
                            System.out.println("groupId: " + group);

                            byte[] valueArray = record.value();
                            if (valueArray != null) {

                                ByteBuffer valueByteBuffer = ByteBuffer.wrap(valueArray);
                                Short valueVersion = valueByteBuffer.getShort();

                                if (valueVersion == 1) {
                                    Struct value = GROUP_METADATA_VALUE_SCHEMA_V1.read(valueByteBuffer);
                                    String protocolType = value.getString(GROUP_METADATA_VALUE_SCHEMA_V1.get
                                            ("protocol_type"));
                                    Integer generation = value.getInt(GROUP_METADATA_VALUE_SCHEMA_V1.get("generation"));
                                    String protocol = value.getString(GROUP_METADATA_VALUE_SCHEMA_V1.get("protocol"));
                                    String leader = value.getString(GROUP_METADATA_VALUE_SCHEMA_V1.get("leader"));
                                    Object[] members = value.getArray(GROUP_METADATA_VALUE_SCHEMA_V1.get("members"));
                                    System.out.println("protocolType: " + protocolType + " generation: " + generation
                                            + " protocol: " + protocol + " leader: " + leader);
                                    for (Object object : members) {
                                        if (object instanceof Struct) {
                                            Struct struct = (Struct) object;
                                            String memberId = struct.getString(MEMBER_METADATA_V1.get("member_id"));
                                            String clientId = struct.getString(MEMBER_METADATA_V1.get("client_id"));
                                            String clientHost = struct.getString(MEMBER_METADATA_V1.get("client_host"));
                                            Integer sessionTimeout = struct.getInt(MEMBER_METADATA_V1.get
                                                    ("session_timeout"));
                                            Integer rebalanceTimeout = struct.getInt(MEMBER_METADATA_V1.get
                                                    ("rebalance_timeout"));
                                            PartitionAssignor.Subscription subscription = ConsumerProtocol
                                                    .deserializeSubscription(struct.getBytes(MEMBER_METADATA_V1.get
                                                            ("subscription")));
                                            PartitionAssignor.Assignment assignment = ConsumerProtocol
                                                    .deserializeAssignment(struct.getBytes(MEMBER_METADATA_V1.get
                                                            ("assignment")));
                                            System.out.println("memberId: " + memberId + " clientId: " + clientId + "" +
                                                    " clientHost: " + clientHost + " sessionTimeout: " +
                                                    sessionTimeout + " rebalanceTimeout: " + rebalanceTimeout + " " +
                                                    "subscription: " + subscription.toString() + " assignment: " +
                                                    assignment.toString());
                                        } else {
                                            System.out.println("Object " + object + " is not variable of Struct");
                                            System.exit(1);
                                        }
                                    }
                                } else if (valueVersion == 0) {
                                    System.out.println("Encountered GroupMetadata Value Version " + valueVersion);
                                    System.exit(1);
                                } else {
                                    System.out.println("Unrecognized GroupMetadata Value Version " + valueVersion);
                                    System.exit(1);
                                }
                            } else {
                                System.out.println("No idea in which cases value will be null");
                                //encountered this case
                            }
                        } else if (version == 1) {
                            Struct key = OFFSET_COMMIT_KEY_SCHEMA.read(byteBuffer);
                            String group = key.getString(OFFSET_COMMIT_KEY_SCHEMA.get("group"));
                            String topic = key.getString(OFFSET_COMMIT_KEY_SCHEMA.get("topic"));
                            Integer partition = key.getInt(OFFSET_COMMIT_KEY_SCHEMA.get("partition"));
                            System.out.println("group: " + group + " topic: " + topic + " partition: " + partition);

                            byte[] valueArray = record.value();
                            if (valueArray != null) {
                                ByteBuffer valueByteBuffer = ByteBuffer.wrap(valueArray);
                                Short valueVersion = valueByteBuffer.getShort();
                                if (valueVersion == 1) {
                                    Struct value = OFFSET_COMMIT_VALUE_SCHEMA_V1.read(valueByteBuffer);
                                    Long offset = (Long) value.getLong(OFFSET_COMMIT_VALUE_SCHEMA_V1.get("offset"));
                                    String metadata = value.getString(OFFSET_COMMIT_VALUE_SCHEMA_V1.get("metadata"));
                                    Long commitTimestamp = (Long) value.getLong(OFFSET_COMMIT_VALUE_SCHEMA_V1.get
                                            ("commit_timestamp"));
                                    Long expireTimestamp = (Long) value.getLong(OFFSET_COMMIT_VALUE_SCHEMA_V1.get
                                            ("expire_timestamp"));

                                    System.out.println("offset: " + offset + " metadata: " + metadata + " " +
                                            "commitTimestamp: " + commitTimestamp + " expireTimestamp: " +
                                            expireTimestamp);

                                    GroupTopicPartition groupTopicPartition = new GroupTopicPartition(group, topic,
                                            partition);
                                    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset, metadata);
                                    groupTopicPartitionOffsetAndMetadataMap.put(groupTopicPartition, offsetAndMetadata);
                                    if (!topicConsumerPartitionMap.containsKey(topic)) {
                                        topicConsumerPartitionMap.put(topic, new ConcurrentHashMap<>());
                                        topicConsumerPartitionMap.get(topic).put(group, new ConcurrentHashMap<>());
                                        topicConsumerPartitionMap.get(topic).get(group).put(partition, new Object());
                                    } else if (!topicConsumerPartitionMap.get(topic).containsKey(group)) {
                                        topicConsumerPartitionMap.get(topic).put(group, new ConcurrentHashMap<>());
                                        topicConsumerPartitionMap.get(topic).get(group).put(partition, new Object());
                                    } else if (!topicConsumerPartitionMap.get(topic).get(group).containsKey
                                            (partition)) {
                                        topicConsumerPartitionMap.get(topic).get(group).put(partition, new Object());
                                    }
                                    if (!consumerTopicPartitionMap.containsKey(group)) {
                                        consumerTopicPartitionMap.put(group, new ConcurrentHashMap<>());
                                        consumerTopicPartitionMap.get(group).put(topic, new ConcurrentHashMap<>());
                                        consumerTopicPartitionMap.get(group).get(topic).put(partition, new Object());
                                    } else if (!consumerTopicPartitionMap.get(group).containsKey(topic)) {
                                        consumerTopicPartitionMap.get(group).put(topic, new ConcurrentHashMap<>());
                                        consumerTopicPartitionMap.get(group).get(topic).put(partition, new Object());
                                    } else if (!consumerTopicPartitionMap.get(group).get(topic).containsKey
                                            (partition)) {
                                        consumerTopicPartitionMap.get(group).get(topic).put(partition, new Object());
                                    }
                                } else if (valueVersion == 0) {
                                    Struct value = OFFSET_COMMIT_VALUE_SCHEMA_V0.read(valueByteBuffer);
                                    Long offset = (Long) value.getLong(OFFSET_COMMIT_VALUE_SCHEMA_V0.get("offset"));
                                    String metadata = value.getString(OFFSET_COMMIT_VALUE_SCHEMA_V0.get("metadata"));
                                    Long timeStamp = (Long) value.getLong(OFFSET_COMMIT_VALUE_SCHEMA_V0.get
                                            ("timestamp"));

                                    System.out.println("offset: " + offset + " metadata: " + metadata + " " +
                                            "timestamp: " + timeStamp);
                                    System.out.println("Encountered Offset value version " + valueVersion + " for " +
                                            "offset value");
                                    System.exit(1);
                                } else {
                                    System.out.println("Unrecognized Offset Value Version " + valueVersion);
                                    System.exit(1);
                                }
                            } else {
                                System.out.println("Offset for Consumer group " + group + ", topic " + topic + ", and" +
                                        " partition " + partition + " is expired");
                                GroupTopicPartition groupTopicPartition = new GroupTopicPartition(group, topic,
                                        partition);
                                if (groupTopicPartitionOffsetAndMetadataMap.containsKey(groupTopicPartition)) {
                                    groupTopicPartitionOffsetAndMetadataMap.remove(groupTopicPartition);
                                } else {
                                    System.out.println("How can this case even occur");
                                    System.exit(1);
                                }
                                nullCheck(topicConsumerPartitionMap, topic, group, partition);
                                nullCheck(consumerTopicPartitionMap, group, topic, partition);
                            }

                        } else {
                            System.out.println("Unrecognized Version " + version);
                            System.exit(1);
                        }
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                        e.printStackTrace();
                        System.exit(1);
                    }
                }
            }
        }

        private void nullCheck(Map<String, Map<String, Map<Integer, Object>>> map, String first, String second,
                               Integer partition) {
            if (map.containsKey(first)) {
                if (map.get(first).containsKey(second)) {
                    if (map.get(first).get(second).containsKey(partition)) {
                        map.get(first).get(second).remove(partition);
                        if (map.get(first).get(second).size() == 0) {
                            map.get(first).remove(second);
                            if (map.get(first).size() == 0) {
                                map.remove(first);
                            }
                        }
                    } else {
                        System.out.println("How can this case even occur that is no partition");
                        System.exit(1);
                    }
                } else {
                    System.out.println("How can this case even occur that is no second");
                    System.exit(1);
                }
            } else {
                System.out.println("How can this case even occur that is no first");
                System.exit(1);
            }
        }
    }

    public class GetConsumerDetailsUsingStreams {
        private final static String TOPIC = "jaasacct-test";
        private final static String BOOTSTRAP_SERVERS = "slc11bmw.us.oracle.com:9092";
        private final static String stateStoreName = "state-store";
        private final static String repartitionStateStoreName = "global-state-store";
        private final static boolean isRepartitioningNeeded = false;
        private final static boolean isTesting = true;

        public void intializeAndStartStreamingData() {
            if(isTesting) {
                KafkaConsumerGroups.isTesting = isTesting;
            }
            Properties config = streamsConfig(Serdes.ByteArray().getClass(), Serdes.ByteArray().getClass(),
                    "monitor-app");
            TopologyBuilder topologyBuilder = new TopologyBuilder();

            StateStoreSupplier stateStoreSupplier = null;
            if(!isRepartitioningNeeded) {
                stateStoreSupplier = Stores.create(stateStoreName).withByteArrayKeys()
                    .withByteArrayValues()
                    .persistent().build();
            } else {
                stateStoreSupplier = Stores.create(repartitionStateStoreName).withByteArrayKeys()
                        .withByteArrayValues().persistent().build();
            }


            if (!isRepartitioningNeeded) {
                topologyBuilder.addSource("CONSUMER_OFFSETS_SOURCE",
                        CONSUMER_OFFSETS).addProcessor("ConsumerOffsetsTopicProcessor", () -> new
                        ConsumerOffsetsTopicProcessor(stateStoreName), new String[]{"CONSUMER_OFFSETS_SOURCE"})
                        .addStateStore(stateStoreSupplier, "ConsumerOffsetsTopicProcessor");
            } else {
                topologyBuilder.addSource("CONSUMER_OFFSETS_SOURCE",
                        CONSUMER_OFFSETS).addProcessor("ConsumerOffsetsTopicProcessor", () -> new
                        ConsumerOffsetsTopicProcessor(repartitionStateStoreName), new String[]{"CONSUMER_OFFSETS_SOURCE"});
            }
            KafkaStreams kafkaStreams = new KafkaStreams(topologyBuilder, config);
            kafkaStreams.start();

            StateStoreQueryThread stateStoreQueryThread = new StateStoreQueryThread(kafkaStreams, repartitionStateStoreName,
                    keySchemaInJSON, valueSchemaInJSON);
            Thread thread = new Thread(stateStoreQueryThread);
            thread.start();
        }

        public Properties streamsConfig(Class<? extends Serde> keySerdes, Class<? extends Serde> valueSerdes,
                                        String applicationId) {
            Properties config = new Properties();
            config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
            config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            config.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, keySerdes);
            // above config is deprecated, please check DEFAULT+above config in other libraries.
            config.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, valueSerdes);
            // above config is deprecated, please check DEFAULT+above config in other libraries.
            config.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG), 60000);
            config.put(StreamsConfig.consumerPrefix(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG), "false");
            //increased interval because it is ok even if we consume one record multiple times.
            config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0);
            //if there are multiple monitor app instances, we can maintain replicas so that there will be
            //very less down time.
            return config;
        }
    }

    class StateStoreQueryThread<T, V> implements Runnable {

        private KafkaStreams streams;
        private String storeName;
        public final AvroTest avroTest = new AvroTest();
        String keySchema;
        String valueSchema;

        StateStoreQueryThread(KafkaStreams streams, String storeName, String keySchema, String valueSchema) {
            this.streams = streams;
            this.storeName = storeName;
            this.keySchema = keySchema;
            this.valueSchema = valueSchema;
        }

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
        /*try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
       /* while(streams.state() != KafkaStreams.State.RUNNING) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }*/
            ReadOnlyKeyValueStore<byte[], V> keyValueStore = null;
            try {
                keyValueStore = waitUntilStoreIsQueryable(storeName, QueryableStoreTypes.keyValueStore(), streams);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while (true) {
                 /*streams.store(storeName, QueryableStoreTypes.keyValueStore());*/

                /*LOG.info
                ("------------------------------------------------------------------------------------------------");
                LOG.info("WordsWithCountsTopic value " + avroTest.deSerialize((byte[])keyValueStore.get(avroTest
                .serialize(1,"WordsWithCountsTopic", null)), 1, valueSchemaInJSON));

                LOG.info
                ("------------------------------------------------------------------------------------------------");
                KeyValueIterator<byte[], V> range1 = keyValueStore.range(avroTest.serialize(1,"WordsWithCountsTopic",
                 null), avroTest.serialize(1,"WordsWithCountsTopic", null));
                while (range1.hasNext()) {
                    KeyValue<byte[], V> next = range1.next();
                    if (avroTest.getVersion((byte[]) next.key) == 1) {
                        LOG.info("key is " + avroTest.deSerialize(String.class, (byte[]) next.key, 1) + " and value is "
                                + avroTest.deSerialize((byte[]) next.value, 1, valueSchema).toString());

                        *//*System.out.println("key is " + avroTest.deSerialize(String.class, (byte[]) next.key, 1) +
                         *  " " +
                                "and " +
                                "value is "
                                + avroTest.deSerialize((byte[]) next.value, 1, valueSchema).toString());*//*
                    }
                }*/

                LOG.info("------------------------------------------------------------------------------------------------");
                KeyValueIterator<byte[], V> range = keyValueStore.all();
                while (range.hasNext()) {
                    KeyValue<byte[], V> next = range.next();
                    if (avroTest.getVersion((byte[]) next.key) == 1) {
                        LOG.info("key is " + avroTest.deSerialize(String.class, (byte[]) next.key, 1) + " and value is "
                                + avroTest.deSerialize((byte[]) next.value, 1, valueSchema).toString());

                        /*System.out.println("key is " + avroTest.deSerialize(String.class, (byte[]) next.key, 1) + "
                         " +
                                "and " +
                                "value is "
                                + avroTest.deSerialize((byte[]) next.value, 1, valueSchema).toString());*/
                    }
                }

                LOG.info("------------------------------------------------------------------------------------------------");
            }
        }

        public <T> T waitUntilStoreIsQueryable(final String storeName,
                                               final QueryableStoreType<T> queryableStoreType,
                                               final KafkaStreams streams) throws InterruptedException {
            while (true) {
                try {
                    return streams.store(storeName, queryableStoreType);
                } catch (InvalidStateStoreException ignored) {
                    // store not yet ready for querying
                    Thread.sleep(100);
                }
            }
        }
    }

    public class ConsumerOffsetsTopicProcessor implements Processor<byte[], byte[]> {
        private ProcessorContext context;
        private KeyValueStore<byte[], byte[]> kvStore;
        private String stateStoreName;
        private final int TOPIC_TO_CONSUMER_GROUP_MAP_KEY_VERSION = 1;
        private final int TOPIC_TO_CONSUMER_GROUP_MAP_VALUE_VERSION = 1;
        private final int CONSUMER_GROUP_TO_TOPIC_MAP_KEY_VERSION = 2;
        private final int CONSUMER_GROUP_TO_TOPIC_MAP_VALUE_VERSION = 1;

        public ConsumerOffsetsTopicProcessor(String stateStoreName) {
            this.stateStoreName = stateStoreName;
        }

        /**
         * Initialize this processor with the given context. The framework ensures this is called once per processor
         * when the topology
         * that contains it is initialized.
         * <p>
         * If this processor is to be {@link #punctuate(long) called periodically} by the framework, then this method
         * should
         * {@link ProcessorContext#schedule(long) schedule itself} with the provided context.
         *
         * @param context the context; may not be null
         */
        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.kvStore = (KeyValueStore<byte[], byte[]>) context.getStateStore(stateStoreName);
        }

        /**
         * Process the record with the given key and value.
         *
         * @param keyByteArray   the key for the record
         * @param valueByteArray the value for the record
         */
        @Override
        public void process(byte[] keyByteArray, byte[] valueByteArray) {
            try {
                ByteBuffer byteBufferKey = ByteBuffer.wrap(keyByteArray);
                int keyVersion = byteBufferKey.getShort();
                if (keyVersion == 1) {
                    Struct key = OFFSET_COMMIT_KEY_SCHEMA.read(byteBufferKey);
                    String group = key.getString(OFFSET_COMMIT_KEY_SCHEMA.get("group"));
                    String topic = key.getString(OFFSET_COMMIT_KEY_SCHEMA.get("topic"));
                    Integer partition = key.getInt(OFFSET_COMMIT_KEY_SCHEMA.get("partition"));
                    byte[] topicKeyByteArray = avroTest.serialize(TOPIC_TO_CONSUMER_GROUP_MAP_KEY_VERSION, topic, null);
                    byte[] consumerGroupKeyByteArray = avroTest.serialize(CONSUMER_GROUP_TO_TOPIC_MAP_KEY_VERSION,
                            group,

                            null);
                    //this is done because paritition is map key and map key must be string.
                    String partitionString = partition.toString();
                    System.out.println("group: " + group + " topic: " + topic + " partition: " + partition);

                    if (valueByteArray != null) {
                        ByteBuffer valueByteBuffer = ByteBuffer.wrap(valueByteArray);
                        Short valueVersion = valueByteBuffer.getShort();
                        if (valueVersion == 1) {
                            Struct value = OFFSET_COMMIT_VALUE_SCHEMA_V1.read(valueByteBuffer);
                            Long offset = (Long) value.getLong(OFFSET_COMMIT_VALUE_SCHEMA_V1.get("offset"));
                            String metadata = value.getString(OFFSET_COMMIT_VALUE_SCHEMA_V1.get("metadata"));
                            Long commitTimestamp = (Long) value.getLong(OFFSET_COMMIT_VALUE_SCHEMA_V1.get
                                    ("commit_timestamp"));
                            Long expireTimestamp = (Long) value.getLong(OFFSET_COMMIT_VALUE_SCHEMA_V1.get
                                    ("expire_timestamp"));

                            System.out.println("offset: " + offset + " metadata: " + metadata + " " +
                                    "commitTimestamp: " + commitTimestamp + " expireTimestamp: " + expireTimestamp);

                        /*GroupTopicPartition groupTopicPartition = new GroupTopicPartition(group, topic, partition);
                        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset, metadata);
                        groupTopicPartitionOffsetAndMetadataMap.put(groupTopicPartition, offsetAndMetadata);*/
                            byte[] oldConsumerMapValueByteArray = kvStore.get(topicKeyByteArray);
                            if (oldConsumerMapValueByteArray != null) {
                                Map<String, Map<String, Object>> oldConsumerMapValue = (Map<String, Map<String,
                                        Object>>)
                                        avroTest.deSerialize
                                                (oldConsumerMapValueByteArray,
                                                        TOPIC_TO_CONSUMER_GROUP_MAP_VALUE_VERSION,
                                                        valueSchemaInJSON);
                                if (!oldConsumerMapValue.containsKey(group)) {
                                    Map<String, Object> partitionStringMap = new HashMap<>();
                                    partitionStringMap.put(partitionString, null);
                                    oldConsumerMapValue.put(group, partitionStringMap);
                                    byte[] newConsumerMapValueByteArray = avroTest.serialize
                                            (TOPIC_TO_CONSUMER_GROUP_MAP_VALUE_VERSION, oldConsumerMapValue,
                                                    valueSchemaInJSON);
                                    kvStore.put(topicKeyByteArray, newConsumerMapValueByteArray);
                                } else if (!oldConsumerMapValue.get(group).containsKey(partitionString)) {
                                    oldConsumerMapValue.get(group).put(partitionString, null);
                                    byte[] newConsumerMapValueByteArray = avroTest.serialize
                                            (TOPIC_TO_CONSUMER_GROUP_MAP_VALUE_VERSION, oldConsumerMapValue,
                                                    valueSchemaInJSON);
                                    kvStore.put(topicKeyByteArray, newConsumerMapValueByteArray);
                                }
                            } else {
                                Map<String, Object> partitionStringMap = new HashMap<>();
                                partitionStringMap.put(partitionString, null);
                                Map<String, Map<String, Object>> consumerMapValue = new HashMap<>();
                                consumerMapValue.put(group, partitionStringMap);
                                byte[] consumerMapValueByteArray = avroTest.serialize
                                        (TOPIC_TO_CONSUMER_GROUP_MAP_VALUE_VERSION, consumerMapValue,
                                                valueSchemaInJSON);
                                kvStore.put(topicKeyByteArray, consumerMapValueByteArray);
                            }
                            byte[] oldTopicMapValueByteArray = kvStore.get(consumerGroupKeyByteArray);
                            if (oldTopicMapValueByteArray != null) {
                                Map<String, Map<String, Object>> oldTopicMapValue = (Map<String, Map<String, Object>>)
                                        avroTest.deSerialize
                                                (oldTopicMapValueByteArray, CONSUMER_GROUP_TO_TOPIC_MAP_VALUE_VERSION,
                                                        valueSchemaInJSON);
                                if (!oldTopicMapValue.containsKey(topic)) {
                                    Map<String, Object> partitionStringMap = new HashMap<>();
                                    partitionStringMap.put(partitionString, null);
                                    oldTopicMapValue.put(topic, partitionStringMap);
                                    byte[] newTopicMapValueByteArray = avroTest.serialize
                                            (CONSUMER_GROUP_TO_TOPIC_MAP_VALUE_VERSION, oldTopicMapValue,
                                                    valueSchemaInJSON);
                                    kvStore.put(consumerGroupKeyByteArray, newTopicMapValueByteArray);
                                } else if (!oldTopicMapValue.get(topic).containsKey(partitionString)) {
                                    oldTopicMapValue.get(topic).put(partitionString, null);
                                    byte[] newTopicMapValueByteArray = avroTest.serialize
                                            (CONSUMER_GROUP_TO_TOPIC_MAP_VALUE_VERSION, oldTopicMapValue,
                                                    valueSchemaInJSON);
                                    kvStore.put(consumerGroupKeyByteArray, newTopicMapValueByteArray);
                                }
                            } else {
                                Map<String, Object> partitionStringMap = new HashMap<>();
                                partitionStringMap.put(partitionString, null);
                                Map<String, Map<String, Object>> topicMapValue = new HashMap<>();
                                topicMapValue.put(topic, partitionStringMap);
                                byte[] topicMapValueByteArray = avroTest.serialize
                                        (CONSUMER_GROUP_TO_TOPIC_MAP_VALUE_VERSION, topicMapValue, valueSchemaInJSON);
                                kvStore.put(consumerGroupKeyByteArray, topicMapValueByteArray);
                            }

                       /* if (!topicConsumerPartitionMap.containsKey(topic)) {
                            topicConsumerPartitionMap.put(topic, new ConcurrentHashMap<>());
                            topicConsumerPartitionMap.get(topic).put(group, new ConcurrentHashMap<>());
                            topicConsumerPartitionMap.get(topic).get(group).put(partition, new Object());
                        } else if (!topicConsumerPartitionMap.get(topic).containsKey(group)) {
                            topicConsumerPartitionMap.get(topic).put(group, new ConcurrentHashMap<>());
                            topicConsumerPartitionMap.get(topic).get(group).put(partition, new Object());
                        } else if (!topicConsumerPartitionMap.get(topic).get(group).containsKey(partition)) {
                            topicConsumerPartitionMap.get(topic).get(group).put(partition, new Object());
                        }
                        if (!consumerTopicPartitionMap.containsKey(group)) {
                            consumerTopicPartitionMap.put(group, new ConcurrentHashMap<>());
                            consumerTopicPartitionMap.get(group).put(topic, new ConcurrentHashMap<>());
                            consumerTopicPartitionMap.get(group).get(topic).put(partition, new Object());
                        } else if (!consumerTopicPartitionMap.get(group).containsKey(topic)) {
                            consumerTopicPartitionMap.get(group).put(topic, new ConcurrentHashMap<>());
                            consumerTopicPartitionMap.get(group).get(topic).put(partition, new Object());
                        } else if (!consumerTopicPartitionMap.get(group).get(topic).containsKey(partition)) {
                            consumerTopicPartitionMap.get(group).get(topic).put(partition, new Object());
                        }*/
                        } else if (valueVersion == 0) {
                            Struct value = OFFSET_COMMIT_VALUE_SCHEMA_V0.read(valueByteBuffer);
                            Long offset = (Long) value.getLong(OFFSET_COMMIT_VALUE_SCHEMA_V0.get("offset"));
                            String metadata = value.getString(OFFSET_COMMIT_VALUE_SCHEMA_V0.get("metadata"));
                            Long timeStamp = (Long) value.getLong(OFFSET_COMMIT_VALUE_SCHEMA_V0.get
                                    ("timestamp"));

                            System.out.println("offset: " + offset + " metadata: " + metadata + " " +
                                    "timestamp: " + timeStamp);
                            System.out.println("Encountered Offset value version " + valueVersion + " for offset " +
                                    "value");
                            System.exit(1);
                        } else {
                            System.out.println("Unrecognized Offset Value Version " + valueVersion);
                            System.exit(1);
                        }
                    } else {
                        System.out.println("Offset for Consumer group " + group + ", topic " + topic + ", and " +
                                "partition "
                                + partitionString + " is expired");/*
                    GroupTopicPartition groupTopicPartition = new GroupTopicPartition(group, topic, partition);
                    if (groupTopicPartitionOffsetAndMetadataMap.containsKey(groupTopicPartition)) {
                        groupTopicPartitionOffsetAndMetadataMap.remove(groupTopicPartition);
                    } else {
                        System.out.println("How can this case even occur");
                        System.exit(1);
                    }*/
                    /*nullCheck(topicConsumerPartitionMap, topic, group, partition);
                    nullCheck(consumerTopicPartitionMap, group, topic, partition);*/
                    if(!isTesting) {
                        nullCheck(kvStore, topicKeyByteArray, group, partitionString,
                                TOPIC_TO_CONSUMER_GROUP_MAP_VALUE_VERSION);
                        nullCheck(kvStore, consumerGroupKeyByteArray, topic, partitionString,
                                CONSUMER_GROUP_TO_TOPIC_MAP_VALUE_VERSION);
                    }
                    }

                } else if (keyVersion == 2) {
                    System.out.println("Ignoring Consumer Metadata Version " + keyVersion);
                } else {
                    System.out.println("Unrecognized Version " + keyVersion);
                    System.exit(1);
                }
            } catch (Exception e) {
                System.out.println("Catched exception for a record " + e.getMessage());
                e.printStackTrace();
                System.exit(1);
            }
        }

        public void nullCheck(KeyValueStore<byte[], byte[]> kvStore, byte[] firstKey, String
                secondKey, String thirdKey, int valueVersion) {
            byte[] valueInByteArray = kvStore.get(firstKey);
            if (valueInByteArray != null) {
                Map<String, Map<String, Object>> value = (Map<String, Map<String, Object>>) avroTest.deSerialize
                        (valueInByteArray, valueVersion, valueSchemaInJSON);
                if (value.containsKey(secondKey)) {
                    if (value.get(secondKey).containsKey(thirdKey)) {
                        value.get(secondKey).remove(thirdKey);
                        if (value.get(secondKey).size() == 0) {
                            value.remove(secondKey);
                            if (value.size() == 0) {
                                kvStore.delete(firstKey);
                                return;
                            }
                            kvStore.put(firstKey, avroTest.serialize(valueVersion, value, valueSchemaInJSON));
                            return;
                        }
                        kvStore.put(firstKey, avroTest.serialize(valueVersion, value, valueSchemaInJSON));
                        return;
                    } else {
                        System.out.println("How can this case even occur that is no partition");
                        System.exit(1);
                    }
                } else {
                    System.out.println("How can this case even occur that is no second");
                    System.exit(1);
                }
            } else {
                System.out.println("How can this case even occur that is no first");
                System.exit(1);
            }
        }

        /**
         * Perform any periodic operations, if this processor {@link ProcessorContext#schedule(long) schedule itself}
         * with the context
         * during {@link #init(ProcessorContext) initialization}.
         *
         * @param timestamp the stream time when this method is being called
         */
        @Override
        public void punctuate(long timestamp) {

        }

        /**
         * Close this processor and clean up any resources. Be aware that {@link #close()} is called after an
         * internal cleanup.
         * Thus, it is not possible to write anything to Kafka as underlying clients are already closed.
         */
        @Override
        public void close() {

        }
    }

    public class ConsumerOffsetsTopicProcessorUsingTwoTopics implements Processor<byte[], byte[]> {
        private ProcessorContext context;
        private KeyValueStore<byte[], byte[]> kvStore;
        private String stateStoreName;
        private final int TOPIC_TO_CONSUMER_GROUP_MAP_KEY_VERSION = 1;
        private final int TOPIC_TO_CONSUMER_GROUP_MAP_VALUE_VERSION = 1;
        private final int CONSUMER_GROUP_TO_TOPIC_MAP_KEY_VERSION = 2;
        private final int CONSUMER_GROUP_TO_TOPIC_MAP_VALUE_VERSION = 1;

        public ConsumerOffsetsTopicProcessorUsingTwoTopics(String stateStoreName) {
            this.stateStoreName = stateStoreName;
        }

        /**
         * Initialize this processor with the given context. The framework ensures this is called once per processor
         * when the topology
         * that contains it is initialized.
         * <p>
         * If this processor is to be {@link #punctuate(long) called periodically} by the framework, then this method
         * should
         * {@link ProcessorContext#schedule(long) schedule itself} with the provided context.
         *
         * @param context the context; may not be null
         */
        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.kvStore = (KeyValueStore<byte[], byte[]>) context.getStateStore(stateStoreName);
        }

        /**
         * Process the record with the given key and value.
         *
         * @param keyByteArray   the key for the record
         * @param valueByteArray the value for the record
         */
        @Override
        public void process(byte[] keyByteArray, byte[] valueByteArray) {
            try {
                ByteBuffer byteBufferKey = ByteBuffer.wrap(keyByteArray);
                int keyVersion = byteBufferKey.getShort();
                if (keyVersion == 1) {
                    Struct key = OFFSET_COMMIT_KEY_SCHEMA.read(byteBufferKey);
                    String group = key.getString(OFFSET_COMMIT_KEY_SCHEMA.get("group"));
                    String topic = key.getString(OFFSET_COMMIT_KEY_SCHEMA.get("topic"));
                    Integer partition = key.getInt(OFFSET_COMMIT_KEY_SCHEMA.get("partition"));
                    byte[] topicKeyByteArray = avroTest.serialize(TOPIC_TO_CONSUMER_GROUP_MAP_KEY_VERSION, topic, null);
                    byte[] consumerGroupKeyByteArray = avroTest.serialize(CONSUMER_GROUP_TO_TOPIC_MAP_KEY_VERSION,
                            group,

                            null);
                    //this is done because paritition is map key and map key must be string.
                    String partitionString = partition.toString();
                    System.out.println("group: " + group + " topic: " + topic + " partition: " + partition);

                    if (valueByteArray != null) {
                        ByteBuffer valueByteBuffer = ByteBuffer.wrap(valueByteArray);
                        Short valueVersion = valueByteBuffer.getShort();
                        if (valueVersion == 1) {
                            Struct value = OFFSET_COMMIT_VALUE_SCHEMA_V1.read(valueByteBuffer);
                            Long offset = (Long) value.getLong(OFFSET_COMMIT_VALUE_SCHEMA_V1.get("offset"));
                            String metadata = value.getString(OFFSET_COMMIT_VALUE_SCHEMA_V1.get("metadata"));
                            Long commitTimestamp = (Long) value.getLong(OFFSET_COMMIT_VALUE_SCHEMA_V1.get
                                    ("commit_timestamp"));
                            Long expireTimestamp = (Long) value.getLong(OFFSET_COMMIT_VALUE_SCHEMA_V1.get
                                    ("expire_timestamp"));

                            System.out.println("offset: " + offset + " metadata: " + metadata + " " +
                                    "commitTimestamp: " + commitTimestamp + " expireTimestamp: " + expireTimestamp);

                        /*GroupTopicPartition groupTopicPartition = new GroupTopicPartition(group, topic, partition);
                        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset, metadata);
                        groupTopicPartitionOffsetAndMetadataMap.put(groupTopicPartition, offsetAndMetadata);*/
                            byte[] oldConsumerMapValueByteArray = kvStore.get(topicKeyByteArray);
                            if (oldConsumerMapValueByteArray != null) {
                                Map<String, Map<String, Object>> oldConsumerMapValue = (Map<String, Map<String,
                                        Object>>)
                                        avroTest.deSerialize
                                                (oldConsumerMapValueByteArray,
                                                        TOPIC_TO_CONSUMER_GROUP_MAP_VALUE_VERSION,
                                                        valueSchemaInJSON);
                                if (!oldConsumerMapValue.containsKey(group)) {
                                    Map<String, Object> partitionStringMap = new HashMap<>();
                                    partitionStringMap.put(partitionString, null);
                                    oldConsumerMapValue.put(group, partitionStringMap);
                                    byte[] newConsumerMapValueByteArray = avroTest.serialize
                                            (TOPIC_TO_CONSUMER_GROUP_MAP_VALUE_VERSION, oldConsumerMapValue,
                                                    valueSchemaInJSON);
                                    kvStore.put(topicKeyByteArray, newConsumerMapValueByteArray);
                                } else if (!oldConsumerMapValue.get(group).containsKey(partitionString)) {
                                    oldConsumerMapValue.get(group).put(partitionString, null);
                                    byte[] newConsumerMapValueByteArray = avroTest.serialize
                                            (TOPIC_TO_CONSUMER_GROUP_MAP_VALUE_VERSION, oldConsumerMapValue,
                                                    valueSchemaInJSON);
                                    kvStore.put(topicKeyByteArray, newConsumerMapValueByteArray);
                                }
                            } else {
                                Map<String, Object> partitionStringMap = new HashMap<>();
                                partitionStringMap.put(partitionString, null);
                                Map<String, Map<String, Object>> consumerMapValue = new HashMap<>();
                                consumerMapValue.put(group, partitionStringMap);
                                byte[] consumerMapValueByteArray = avroTest.serialize
                                        (TOPIC_TO_CONSUMER_GROUP_MAP_VALUE_VERSION, consumerMapValue,
                                                valueSchemaInJSON);
                                kvStore.put(topicKeyByteArray, consumerMapValueByteArray);
                            }
                            byte[] oldTopicMapValueByteArray = kvStore.get(consumerGroupKeyByteArray);
                            if (oldTopicMapValueByteArray != null) {
                                Map<String, Map<String, Object>> oldTopicMapValue = (Map<String, Map<String, Object>>)
                                        avroTest.deSerialize
                                                (oldTopicMapValueByteArray, CONSUMER_GROUP_TO_TOPIC_MAP_VALUE_VERSION,
                                                        valueSchemaInJSON);
                                if (!oldTopicMapValue.containsKey(topic)) {
                                    Map<String, Object> partitionStringMap = new HashMap<>();
                                    partitionStringMap.put(partitionString, null);
                                    oldTopicMapValue.put(topic, partitionStringMap);
                                    byte[] newTopicMapValueByteArray = avroTest.serialize
                                            (CONSUMER_GROUP_TO_TOPIC_MAP_VALUE_VERSION, oldTopicMapValue,
                                                    valueSchemaInJSON);
                                    kvStore.put(consumerGroupKeyByteArray, newTopicMapValueByteArray);
                                } else if (!oldTopicMapValue.get(topic).containsKey(partitionString)) {
                                    oldTopicMapValue.get(topic).put(partitionString, null);
                                    byte[] newTopicMapValueByteArray = avroTest.serialize
                                            (CONSUMER_GROUP_TO_TOPIC_MAP_VALUE_VERSION, oldTopicMapValue,
                                                    valueSchemaInJSON);
                                    kvStore.put(consumerGroupKeyByteArray, newTopicMapValueByteArray);
                                }
                            } else {
                                Map<String, Object> partitionStringMap = new HashMap<>();
                                partitionStringMap.put(partitionString, null);
                                Map<String, Map<String, Object>> topicMapValue = new HashMap<>();
                                topicMapValue.put(topic, partitionStringMap);
                                byte[] topicMapValueByteArray = avroTest.serialize
                                        (CONSUMER_GROUP_TO_TOPIC_MAP_VALUE_VERSION, topicMapValue, valueSchemaInJSON);
                                kvStore.put(consumerGroupKeyByteArray, topicMapValueByteArray);
                            }

                       /* if (!topicConsumerPartitionMap.containsKey(topic)) {
                            topicConsumerPartitionMap.put(topic, new ConcurrentHashMap<>());
                            topicConsumerPartitionMap.get(topic).put(group, new ConcurrentHashMap<>());
                            topicConsumerPartitionMap.get(topic).get(group).put(partition, new Object());
                        } else if (!topicConsumerPartitionMap.get(topic).containsKey(group)) {
                            topicConsumerPartitionMap.get(topic).put(group, new ConcurrentHashMap<>());
                            topicConsumerPartitionMap.get(topic).get(group).put(partition, new Object());
                        } else if (!topicConsumerPartitionMap.get(topic).get(group).containsKey(partition)) {
                            topicConsumerPartitionMap.get(topic).get(group).put(partition, new Object());
                        }
                        if (!consumerTopicPartitionMap.containsKey(group)) {
                            consumerTopicPartitionMap.put(group, new ConcurrentHashMap<>());
                            consumerTopicPartitionMap.get(group).put(topic, new ConcurrentHashMap<>());
                            consumerTopicPartitionMap.get(group).get(topic).put(partition, new Object());
                        } else if (!consumerTopicPartitionMap.get(group).containsKey(topic)) {
                            consumerTopicPartitionMap.get(group).put(topic, new ConcurrentHashMap<>());
                            consumerTopicPartitionMap.get(group).get(topic).put(partition, new Object());
                        } else if (!consumerTopicPartitionMap.get(group).get(topic).containsKey(partition)) {
                            consumerTopicPartitionMap.get(group).get(topic).put(partition, new Object());
                        }*/
                        } else if (valueVersion == 0) {
                            Struct value = OFFSET_COMMIT_VALUE_SCHEMA_V0.read(valueByteBuffer);
                            Long offset = (Long) value.getLong(OFFSET_COMMIT_VALUE_SCHEMA_V0.get("offset"));
                            String metadata = value.getString(OFFSET_COMMIT_VALUE_SCHEMA_V0.get("metadata"));
                            Long timeStamp = (Long) value.getLong(OFFSET_COMMIT_VALUE_SCHEMA_V0.get
                                    ("timestamp"));

                            System.out.println("offset: " + offset + " metadata: " + metadata + " " +
                                    "timestamp: " + timeStamp);
                            System.out.println("Encountered Offset value version " + valueVersion + " for offset " +
                                    "value");
                            System.exit(1);
                        } else {
                            System.out.println("Unrecognized Offset Value Version " + valueVersion);
                            System.exit(1);
                        }
                    } else {
                        System.out.println("Offset for Consumer group " + group + ", topic " + topic + ", and " +
                                "partition "
                                + partitionString + " is expired");/*
                    GroupTopicPartition groupTopicPartition = new GroupTopicPartition(group, topic, partition);
                    if (groupTopicPartitionOffsetAndMetadataMap.containsKey(groupTopicPartition)) {
                        groupTopicPartitionOffsetAndMetadataMap.remove(groupTopicPartition);
                    } else {
                        System.out.println("How can this case even occur");
                        System.exit(1);
                    }*/
                    /*nullCheck(topicConsumerPartitionMap, topic, group, partition);
                    nullCheck(consumerTopicPartitionMap, group, topic, partition);*/
                        if(!isTesting) {
                            nullCheck(kvStore, topicKeyByteArray, group, partitionString,
                                    TOPIC_TO_CONSUMER_GROUP_MAP_VALUE_VERSION);
                            nullCheck(kvStore, consumerGroupKeyByteArray, topic, partitionString,
                                    CONSUMER_GROUP_TO_TOPIC_MAP_VALUE_VERSION);
                        }
                    }

                } else if (keyVersion == 2) {
                    System.out.println("Ignoring Consumer Metadata Version " + keyVersion);
                } else {
                    System.out.println("Unrecognized Version " + keyVersion);
                    System.exit(1);
                }
            } catch (Exception e) {
                System.out.println("Catched exception for a record " + e.getMessage());
                e.printStackTrace();
                System.exit(1);
            }
        }

        public void nullCheck(KeyValueStore<byte[], byte[]> kvStore, byte[] firstKey, String
                secondKey, String thirdKey, int valueVersion) {
            byte[] valueInByteArray = kvStore.get(firstKey);
            if (valueInByteArray != null) {
                Map<String, Map<String, Object>> value = (Map<String, Map<String, Object>>) avroTest.deSerialize
                        (valueInByteArray, valueVersion, valueSchemaInJSON);
                if (value.containsKey(secondKey)) {
                    if (value.get(secondKey).containsKey(thirdKey)) {
                        value.get(secondKey).remove(thirdKey);
                        if (value.get(secondKey).size() == 0) {
                            value.remove(secondKey);
                            if (value.size() == 0) {
                                kvStore.delete(firstKey);
                                return;
                            }
                            kvStore.put(firstKey, avroTest.serialize(valueVersion, value, valueSchemaInJSON));
                            return;
                        }
                        kvStore.put(firstKey, avroTest.serialize(valueVersion, value, valueSchemaInJSON));
                        return;
                    } else {
                        System.out.println("How can this case even occur that is no partition");
                        System.exit(1);
                    }
                } else {
                    System.out.println("How can this case even occur that is no second");
                    System.exit(1);
                }
            } else {
                System.out.println("How can this case even occur that is no first");
                System.exit(1);
            }
        }

        /**
         * Perform any periodic operations, if this processor {@link ProcessorContext#schedule(long) schedule itself}
         * with the context
         * during {@link #init(ProcessorContext) initialization}.
         *
         * @param timestamp the stream time when this method is being called
         */
        @Override
        public void punctuate(long timestamp) {

        }

        /**
         * Close this processor and clean up any resources. Be aware that {@link #close()} is called after an
         * internal cleanup.
         * Thus, it is not possible to write anything to Kafka as underlying clients are already closed.
         */
        @Override
        public void close() {

        }
    }
}

class GroupTopicPartition {
    private String groupName;
    private String topicName;
    private Integer partition;

    GroupTopicPartition(String groupName, String topicName, Integer partition) {
        this.groupName = groupName;
        this.topicName = topicName;
        this.partition = partition;
    }

    /**
     * Returns a hash code value for the object. This method is
     * supported for the benefit of hash tables such as those provided by
     * {@link HashMap}.
     * <p>
     * The general contract of {@code hashCode} is:
     * <ul>
     * <li>Whenever it is invoked on the same object more than once during
     * an execution of a Java application, the {@code hashCode} method
     * must consistently return the same integer, provided no information
     * used in {@code equals} comparisons on the object is modified.
     * This integer need not remain consistent from one execution of an
     * application to another execution of the same application.
     * <li>If two objects are equal according to the {@code equals(Object)}
     * method, then calling the {@code hashCode} method on each of
     * the two objects must produce the same integer result.
     * <li>It is <em>not</em> required that if two objects are unequal
     * according to the {@link Object#equals(Object)}
     * method, then calling the {@code hashCode} method on each of the
     * two objects must produce distinct integer results.  However, the
     * programmer should be aware that producing distinct integer results
     * for unequal objects may improve the performance of hash tables.
     * </ul>
     * <p>
     * As much as is reasonably practical, the hashCode method defined by
     * class {@code Object} does return distinct integers for distinct
     * objects. (This is typically implemented by converting the internal
     * address of the object into an integer, but this implementation
     * technique is not required by the
     * Java&trade; programming language.)
     *
     * @return a hash code value for this object.
     * @see Object#equals(Object)
     * @see System#identityHashCode
     */
    @Override
    public int hashCode() {
        String s = groupName + "/" + topicName + "/" + partition;
        return s.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof GroupTopicPartition) {
            GroupTopicPartition groupTopicPartition = (GroupTopicPartition) obj;
            return this.groupName.equals(groupTopicPartition.groupName) && this.topicName.equals(groupTopicPartition
                    .topicName) && this.partition.equals(groupTopicPartition.partition);
        }
        return super.equals(obj);
    }
}

class MemberMetadata {

}
