import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionSingleton {
  @transient private var instance:SparkSession = null
  def getInstance(sparkConf:SparkConf):SparkSession={
    if(instance==null){
      instance=SparkSession.builder()
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()
    }
    instance
  }
}
