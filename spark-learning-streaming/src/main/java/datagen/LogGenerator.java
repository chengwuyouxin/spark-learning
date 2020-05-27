package datagen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogGenerator {
    private static final Logger logger= LoggerFactory.getLogger(LogGenerator.class);
    public static void main(String[] args){
        int i=0;
        while(true){
            logger.info("The {}th log!",i);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            i++;
        }
    }
}
