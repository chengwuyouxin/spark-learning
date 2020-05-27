package datagen;

/**
 * @programe: SparkExample
 * @description: test
 * @author: lpq
 * @create: 2019-07-29 09:01
 **/
public class Test {
    public static void main(String[] args){
        String s="A|20190306|20190306|2019/3/6 16:58:29 | 06285";
        String[] a=s.split(" \\| ");
        for(String ss:a)
            System.out.println(ss);
    }
}
