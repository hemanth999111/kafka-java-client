//import org.apache.kafka.common.protocol.types.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class StreamsTest {

    private final static String TOPIC = "jaasacct-test";
    private final static String BOOTSTRAP_SERVERS = "slc11bmw.us.oracle.com:9092";
    public final static AvroTest avroTest = new AvroTest();
    static String keySchemaInJSON = "{\"type\" : \"string\"}";
    static String valueSchemaInJSON = "{\"type\" : \"array\", \"items\" : \"string\"}";
    private static final Logger LOG = LoggerFactory.getLogger(StreamsTest.class);
    private static StreamTest1 streamTest1 = new StreamTest1();

    public static void main(final String[] args) throws Exception {
        //wordCount(TOPIC, "WordsWithCountsTopic");
        //aggregateToList();
        Scanner scanner = new Scanner(System.in);
        System.out.println("Starting Stream");
        streamTest1.start();
        System.out.println("Started Stream");
        streamTest1.printAllKeysValues();
        while(true){
            String key = scanner.next();
            String value = streamTest1.getValue(key);
            System.out.println("value for key: " + key + " is " + value);
            if(value == null) {
                System.out.println("I am real null");
            } else if(value.equals("null")) {
                System.out.println("I am string null");
            }
        }
    }

    private static void aggregateToList() {
        String stateStoreName = "dummyStore";
        Properties config = streamsConfig(Serdes.String().getClass(), Serdes.String().getClass(),
                "aggregate-application-id");

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        StateStoreSupplier dummyStore = Stores.create(stateStoreName).withByteArrayKeys().withByteArrayValues()
                .persistent().build();

        topologyBuilder.addSource("SOURCE1",
                "jaasacct-test1").addProcessor("Processor1", () -> new MyProcessor(stateStoreName), new String[]{"SOURCE1"})
                .addStateStore(dummyStore, "Processor1");
        //@throws TopologyBuilderException if state store supplier is already added

        KafkaStreams streams = new KafkaStreams(topologyBuilder, config);
        streams.start();

        /*Semaphore semaphore = new Semaphore(0);
        streams.setStateListener(new KafkaStreams.StateListener() {
            @Override
            public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
                if(newState == KafkaStreams.State.RUNNING) {
                    semaphore.release();
                }
            }
        });*/
        StateStoreQueryThread stateStoreQueryThread = new StateStoreQueryThread(streams, stateStoreName, keySchemaInJSON, valueSchemaInJSON);
        Thread thread = new Thread(stateStoreQueryThread);
        thread.start();
    }

    static class MyProcessor implements Processor<String, String> {

        private ProcessorContext context;
        private KeyValueStore<byte[], byte[]> kvStore;
        private String stateStoreName;
        //private Schema valueSchema = new Schema(new Field("List", new ArrayOf(Type.STRING)));

        MyProcessor(String stateStoreName) {
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
         * @param randomStreamKey   the key for the record
         * @param randomStreamValue the value for the record
         */
        @Override
        public void process(String randomStreamKey, String randomStreamValue) {
            byte[] keyInByteArray = avroTest.serialize(1, randomStreamKey, null);
            byte[] oldValueInByteArray = kvStore.get(keyInByteArray);
            System.out.println("received some message");
            if (oldValueInByteArray != null) {
                Set<String> oldValue = new HashSet<>((List<String>) avroTest.deSerialize(oldValueInByteArray, 1, valueSchemaInJSON));
                oldValue.add(randomStreamValue);
                byte[] newValueInByteArray = avroTest.serialize(1, oldValue, valueSchemaInJSON);
                kvStore.put(keyInByteArray, newValueInByteArray);
                /*ByteBuffer oldValueByteBuffer = ByteBuffer.wrap(oldValue);
                Struct oldValueStruct = valueSchema.read(oldValueByteBuffer);
                String[] oldValueStringArray = (String[]) oldValueStruct.getArray(valueSchema.get("List"));
                Set<String> valueSet = new HashSet<>(Arrays.asList(oldValueStringArray));
                valueSet.add(value);*/
                //handle for cases where value is null
                //newValue = valueSchema.write();
            } else {
                Set<String> newValue = new HashSet<>();
                newValue.add(randomStreamValue);
                byte[] newValueInByteArray = avroTest.serialize(1, newValue, valueSchemaInJSON);
                kvStore.put(keyInByteArray, newValueInByteArray);
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


    public static void wordCount(String sourceTopicName, String sinkTopicName) throws InterruptedException {
        Properties config = streamsConfig(Serdes.String().getClass(), Serdes.String().getClass(), "application-id1");
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> textLines = builder.stream(sourceTopicName);
       /* Create a {@link KStream} from the specified topics.
     * The default {@code "auto.offset.reset"} strategy and default key and value deserializers as specified in the
     * {@link StreamsConfig config} are used.*/
        //multiple topics can be given here, but order of messages from multiple topics is not guranteed.
        KTable<String, Long> wordCounts = textLines
                .flatMapValues(textLine -> {
                    System.out.println(textLine);
                    return Arrays.asList(textLine.toLowerCase().split("\\W+"));
                })
                .groupBy((key, word) -> word)
                .count("Counts");
        wordCounts.to(Serdes.String(), Serdes.Long(), sinkTopicName);

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
        Thread.sleep(10000);
        printFromLocalStateStore(streams);
    }

    public static Properties streamsConfig(Class<? extends Serde> keySerdes, Class<? extends Serde> valueSerdes,
                                           String applicationId) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        config.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, keySerdes);
        // above config is deprecated, please check DEFAULT+above config in other libraries.
        config.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, valueSerdes);
        // above config is deprecated, please check DEFAULT+above config in other libraries.
        config.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG), 60000);
        //increased interval because it is ok even if we consume one record multiple times.
        return config;
    }

    public static void printFromLocalStateStore(KafkaStreams streams) {
        ReadOnlyKeyValueStore<String, Long> keyValueStore =
                streams.store("Counts", QueryableStoreTypes.keyValueStore());
        KeyValueIterator<String, Long> range = keyValueStore.all();
        while (range.hasNext()) {
            KeyValue<String, Long> next = range.next();
            System.out.println("count for " + next.key + ": " + next.value);
        }

    }
}

class StateStoreQueryThread<T, V> implements Runnable{

    private KafkaStreams streams;
    private String storeName;
    public final static AvroTest avroTest = new AvroTest();
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
        ReadOnlyKeyValueStore<T, V> keyValueStore = null;
        try {
            keyValueStore = waitUntilStoreIsQueryable(storeName, QueryableStoreTypes.keyValueStore(), streams);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
                /*streams.store(storeName, QueryableStoreTypes.keyValueStore());*/
        KeyValueIterator<T, V> range = keyValueStore.all();
        while(range.hasNext()) {
            KeyValue<T, V> next = range.next();
            System.out.println("key is " + avroTest.deSerialize(String.class, (byte[])next.key, 1) + " and value is " + avroTest.deSerialize((byte[]) next.value, 1, valueSchema).toString());
        }
    }

    public static <T> T waitUntilStoreIsQueryable(final String storeName,
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

class StreamTest1 {
    ReadOnlyKeyValueStore<String, String> keyValueStore;

    public void start() {
        String stateStoreName = "CURDStateStore";
        Properties config = StreamsTest.streamsConfig(Serdes.String().getClass(), Serdes.String().getClass(),
                "CURDApplication");

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        StateStoreSupplier CURDStateStore = Stores.create(stateStoreName).withStringKeys().withStringValues()
                .persistent().build();

        String sourceTopic = "curdStream";

        topologyBuilder.addSource("SOURCE1",
                sourceTopic).addProcessor("Processor1", () -> new MyProcessor(stateStoreName), "SOURCE1")
                .addStateStore(CURDStateStore, "Processor1");
        //@throws TopologyBuilderException if state store supplier is already added

        KafkaStreams streams = new KafkaStreams(topologyBuilder, config);
        streams.start();

        keyValueStore = null;
        try {
            keyValueStore = StateStoreQueryThread.waitUntilStoreIsQueryable(stateStoreName, QueryableStoreTypes.<String, String>keyValueStore(), streams);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String getValue(String key) {
        return keyValueStore.get(key);
    }

    public void printAllKeysValues() {
        KeyValueIterator<String, String> range = keyValueStore.all();
        while(range.hasNext()) {
            KeyValue<String, String> next = range.next();
            System.out.println("key is " + next.key + " value is " + next.value);
        }
    }

    class MyProcessor implements Processor<String, String> {

        private final String stateStoreName;
        private ProcessorContext context;
        private KeyValueStore<String, String> kvStore;

        MyProcessor(String stateStoreName) {
            this.stateStoreName = stateStoreName;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.kvStore = (KeyValueStore<String, String>) context.getStateStore(stateStoreName);
        }

        @Override
        public void process(String key, String value) {
            kvStore.put(key, value);
        }

        @Override
        public void punctuate(long timestamp) {

        }

        @Override
        public void close() {

        }
    }
}