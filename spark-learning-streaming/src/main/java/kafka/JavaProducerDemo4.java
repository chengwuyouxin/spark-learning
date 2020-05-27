package kafka;
/*
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
*/
import java.util.Properties;

public class JavaProducerDemo4 {
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
        Properties props=new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");

/*
        Schema.Parser parser=new Schema.Parser();
        Schema schema=parser.parse(USER_SCHEMA);
        Injection<GenericRecord,byte[]> recordInjection=
                GenericAvroCodecs.toBinary(schema);
        KafkaProducer<String,byte[]> producer=new KafkaProducer<String,byte[]>(props);

        for(int i=0;i<100;i++){
            GenericData.Record avroRecord=new GenericData.Record(schema);
            avroRecord.put("str1","str1-"+i);
            avroRecord.put("str2","str2-"+i);
            avroRecord.put("int1",i);

            byte[] recordBytes=recordInjection.apply(avroRecord);
            producer.send(new ProducerRecord<String, byte[]>("JavaProducerDemo4",
                    String.valueOf(i),recordBytes));
        }
*/
    }
}
