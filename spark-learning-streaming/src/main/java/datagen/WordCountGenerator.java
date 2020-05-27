package datagen;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * @programe: cashstore
 * @description: test
 * @author: lpq
 * @create: 2019-07-23 14:42
 **/
public class WordCountGenerator {
    public static void main(String[] args){
        Properties prop=new Properties();
        prop.setProperty("bootstrap.servers","dn01:9092,dn02:9092,dn03:9092");
        prop.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(prop);

        int msgNo=1;
        String topic="wordcount";
        String[] words={"spark","hadoop","hbase","hive","redis","mongodb","mysql"};
        Random random=new Random();
        String msg="";
        while(true){
            try{
                msg=words[random.nextInt(6)];
                producer.send(new ProducerRecord<String,String>(topic,String.valueOf(msgNo),msg));
                System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.sss").format(new Date())+" "+msg);
            }catch(Exception ex){
                ex.printStackTrace();
            }
            msgNo++;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
