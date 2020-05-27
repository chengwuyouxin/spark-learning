package kafka;

/*
import com.sun.prism.impl.Disposer;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
*/

import java.util.Properties;

public class JavaConsumerDemo2 {
    private static String USER_SCHEMA="{\n" +
            "    \"type\":\"record\",\n" +
            "    \"name\":\"Customer\",\n" +
            "    \"fields\":[\n" +
            "        {\"name\":\"str1\",\"type\":\"string\"},\n" +
            "        {\"name\":\"str2\",\"type\":\"string\"},\n" +
            "        {\"name\":\"int1\",\"type\":\"int\"}\n" +
            "    ]\n" +
            "}";

    public static void main(String[] args){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("auto.offset.reset", "earliest");
        properties.put("group.id", "jd-group");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

/*
        KafkaConsumer<String,byte[]> consumer=new KafkaConsumer<String, byte[]>(properties);
        consumer.subscribe(Arrays.asList("JavaProducerDemo4"));

        Schema.Parser parser=new Schema.Parser();
        Schema schema=parser.parse(USER_SCHEMA);
        Injection<GenericRecord,byte[]> injection= GenericAvroCodecs.toBinary(schema);

        try{
            while(true){
                ConsumerRecords<String,byte[]> records=consumer.poll(100);
                for(ConsumerRecord<String,byte[]> record:records){
                    GenericRecord record1=injection.invert(record.value()).get();
                    System.out.println("key=" + record.key() + ",str1= " + record1.get("str1")
                            + ", str2= " + record1.get("str2") + ", int1=" + record1.get("int1"));
                }

            }
        }catch(Exception e){
            e.printStackTrace();
        }
*/
    }
}
