import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AvroTest<T> {
    private final EncoderFactory encoderFactory = EncoderFactory.get();
    private final DecoderFactory decoderFactory = DecoderFactory.get();

    public static void main(String args[]) {
        AvroTest avroTest = new AvroTest();
        String stringSchema = "{\"type\" : \"array\", \"items\" : \"string\"}";
        byte[] byteArray = avroTest.serialize(1, Arrays.asList(new String[]{"Hello", "How are you"}), stringSchema);
        List<String> value = (List<String>) avroTest.deSerialize(byteArray, 2, stringSchema);
        System.out.println(value.toString());
    }

    public byte[] serialize(int version, T object, String stringSchema) {
        Schema schema = null;
        if(stringSchema == null) {
            schema = ReflectData.get().getSchema(object.getClass());
        } else {
            schema = new Schema.Parser().parse(stringSchema);
        }
        //Schema schema = ReflectData.get().getSchema(object.getClass());
        //Set set = new HashSet(Arrays.asList(new String[]{"Hello", "How Are You"}));
        //GenericArray avroArray = new GenericData.Array(schema, set);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            outputStream.write(ByteBuffer.allocate(4).putInt(version).array());
        } catch (IOException e) {
            e.printStackTrace();
        }

        BinaryEncoder binaryEncoder = encoderFactory.directBinaryEncoder(outputStream, null);

        DatumWriter writer = new ReflectDatumWriter<>(schema);
        byte[] byteArray = null;
        try {
            writer.write(object, binaryEncoder);
            binaryEncoder.flush();

            byteArray = outputStream.toByteArray();
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error Serializing Avro Message ");
        }
        return byteArray;
    }

    public T deSerialize(byte[] byteArray, int version,  String stringSchema) {
        if (byteArray == null) {
            return null;
        }
        Schema schema = null;
        schema = new Schema.Parser().parse(stringSchema);
        //Schema schema = ReflectData.get().getSchema(tClass);
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
        int versionInByteBuffer = byteBuffer.getInt();
        if (versionInByteBuffer != version) {
            System.out.println("version mismatch while reading data");
        }
        //System.out.println("I am able to enter");
        BinaryDecoder binaryDecoder = decoderFactory.binaryDecoder(byteArray, 4, byteArray.length - 4, null);
        //constructor if writer and reader schema is different
        DatumReader reader = new ReflectDatumReader(schema);
        T object = null;
        try {
            object = (T) reader.read(null, binaryDecoder);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return object;
    }

    public T deSerialize(Class<T> tClass, byte[] byteArray, int version) {
        if (byteArray == null) {
            return null;
        }
        Schema schema = null;
            schema = ReflectData.get().getSchema(tClass);
        //Schema schema = ReflectData.get().getSchema(tClass);
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
        int versionInByteBuffer = byteBuffer.getInt();
        if (versionInByteBuffer != version) {
            System.out.println("version mismatch while reading data");
        }
        //System.out.println("I am able to enter");
        BinaryDecoder binaryDecoder = decoderFactory.binaryDecoder(byteArray, 4, byteArray.length - 4, null);
        //constructor if writer and reader schema is different
        DatumReader reader = new ReflectDatumReader(schema);
        T object = null;
        try {
            object = (T) reader.read(null, binaryDecoder);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return object;
    }

    public int getVersion(byte[]  byteArray) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
        return byteBuffer.getInt();
    }

    public void testing() {

        String userSchema = "{\"type\" : \"array\", \"items\" : \"string\"}";
        Schema schema = new Schema.Parser().parse(userSchema);
        Set set = new HashSet(Arrays.asList(new String[]{"Hello", "How Are You"}));
        GenericArray avroArray = new GenericData.Array(schema, set);

        int schemaVersion = 1;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            outputStream.write(ByteBuffer.allocate(4).putInt(1).array());
        } catch (IOException e) {
            e.printStackTrace();
        }

        BinaryEncoder binaryEncoder = encoderFactory.directBinaryEncoder(outputStream, null);

        DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
        byte[] byteArray = null;
        try {
            writer.write(avroArray, binaryEncoder);
            binaryEncoder.flush();

            byteArray = outputStream.toByteArray();
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error Serializing Avro Message ");
        }

        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
        int version = byteBuffer.getInt();
        if (version == 1) {
            System.out.println("I am able to enter");
            BinaryDecoder binaryDecoder = decoderFactory.binaryDecoder(byteArray, 4, byteArray.length - 4, null);
            //constructor if writer and reader schema is different
            DatumReader reader = new GenericDatumReader(schema);
            try {
                Object object = reader.read(null, binaryDecoder);
                GenericData.Array avroResultArray = (GenericData.Array) object;
                Set resultSet = new HashSet(avroResultArray);
                System.out.println("Result is " + resultSet.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
