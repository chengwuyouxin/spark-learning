package datagen;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * @programe: cashstore
 * @description: CashPlan record producer
 * @author: lpq
 * @create: 2019-07-16 15:00
 **/
public class CashPlanGenerator {

    public static void main(String[] args){
        Properties prop=new Properties();
        prop.setProperty("bootstrap.servers","dn01:9092,dn02:9092,dn03:9092");
        prop.setProperty("key.serializer","org.apache.kafka.common.serialization.IntegerSerializer");
        prop.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<Integer,String> producer=new KafkaProducer<Integer, String>(prop);

        int msgNo=1;
        String topic="CashPlanRecord";
        String[] machineNos={"001","002","003","004","005"};
        String[] currencies={"CNY","USD","JPY","EUR","HKD"};
        String[] branches={"02604","03208","02587","32541"};
        String[] tellers={"0260401","0320801","0258701","3254101"};
        int[] faceValues={10,50,100,500};
        Random random=new Random();
        while(true){
            try{
                CashPlanRecord record=new CashPlanRecord(msgNo,
                        machineNos[random.nextInt(5)],
                        new SimpleDateFormat("yyyy-MM-dd").format(new Date()),
                        currencies[random.nextInt(5)],
                        branches[random.nextInt(4)],
                        tellers[random.nextInt(4)],
                        new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date()),
                        random.nextInt(1000),
                        faceValues[random.nextInt(4)]);
                producer.send(new ProducerRecord<Integer,String>(topic,msgNo,record.toString()));
                System.out.println(record.toString());
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
