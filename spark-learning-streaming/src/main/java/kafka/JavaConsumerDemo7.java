package kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.*;

public class JavaConsumerDemo7 {
    private static Map<TopicPartition,OffsetAndMetadata> currentOffset=new HashMap<TopicPartition, OffsetAndMetadata>();
    public static void main(String[] args){
        Properties props=new Properties();
        props.put("bootstrap.servers", "dn01:9092,dn02:9092,dn03:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset","earliest");
        props.put("group.id","javaconsumerdemo9");
        props.put("auto.commit.offset","false");

        final KafkaConsumer<Integer,String> consumer=new KafkaConsumer<Integer,String>(props);
        try{
            consumer.subscribe(Arrays.asList("JavaProducerDemo2"),
                    new ConsumerRebalanceListener() {
                        //在均衡开始前或消费者停止读取消息之后
                        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                            System.out.println("Lost Patition in rebalance.Committing" +
                                    " current offset:"+currentOffset);
                            consumer.commitSync(currentOffset);
                        }
                        //重新分配分区之后和消费者开始读取消息之前
                        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                            //重新分区之后从开始读取数据
                            System.out.println("Rebalance the partition and begin read message," +
                                    "current offset"+currentOffset);
                            consumer.seekToBeginning(collection);
                        }
                    });
            while(true){
                ConsumerRecords<Integer,String> records=consumer.poll(100);
                for(ConsumerRecord<Integer,String> record:records){
                    System.out.println(String.format("partition: %d,offerset: %d,key:%s,value:%s",
                            record.partition(),record.offset(),record.key(),record.value()));
                    currentOffset.put(new TopicPartition(record.topic(),record.partition()),
                            new OffsetAndMetadata(record.offset()+1,"noMetadate"));
                }
                consumer.commitAsync(currentOffset,null);
            }
        }catch(WakeupException ex){

        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            consumer.close();
        }
    }
}
