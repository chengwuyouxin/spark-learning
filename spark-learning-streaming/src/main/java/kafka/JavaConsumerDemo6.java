package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


//提交特定的偏移量
//自动提交、同步提交或者异步提交只能提交一批消息的最后一个偏移量
// 如果需要在处理消息批次中间提交偏移量，可按如下方式

public class JavaConsumerDemo6 {
    public static void main(String[] args){
        Properties props=new Properties();
        props.put("bootstrap.servers", "dn01:9092,dn02:9092,dn03:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset","earliest");
        props.put("group.id","javaconsumerdemo9");

        props.put("auto.commit.offset","false");

        KafkaConsumer<Integer,String> consumer=new KafkaConsumer<Integer,String>(props);
        consumer.subscribe(Arrays.asList("JavaProducerDemo2"));
        Map<TopicPartition,OffsetAndMetadata> currentOffsets=new HashMap<TopicPartition, OffsetAndMetadata>();
        int count=0;
        try{
            while(true){
                ConsumerRecords<Integer,String> records=consumer.poll(100);
                for(ConsumerRecord<Integer,String> record:records){
                    System.out.println(String.format("partition: %d,offerset: %d,key:%s,value:%s",
                            record.partition(),record.offset(),record.key(),record.value()));
                    currentOffsets.put(new TopicPartition(record.topic(),record.partition()),
                            new OffsetAndMetadata(record.offset()+1,"no MetaData"));
                    if(count%5==0){
                        consumer.commitSync();
                    }
                    count++;
                }
            }
        }finally{
            consumer.close();
        }

    }
}
