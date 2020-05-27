import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object Ex2_JsonSource {
  def main(args:Array[String]): Unit ={
    val spark=SparkSession.builder()
      .appName("Ex2_JsonSource")
      .getOrCreate()

    import spark.implicits._

    val schema=new StructType()
      .add("machineNo",StringType)
      .add("termPaymentDay",StringType)
      .add("currency",StringType)
      .add("branchNo",StringType)
      .add("tellerID",StringType)
      .add("operTime",StringType)
      .add("cashNum",IntegerType)
      .add("faceValue",IntegerType)

    val stream=spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","dn01:9092,dn02:9092,dn03:9092")
      .option("subscribe","CashPlanRecord2")
      .option("startingOffsets","earliest")
      .option("endingOffsets","latest")
      .load()

    val parsed=stream.select(from_json(
      ($"value").cast("string"),schema).alias("parsed_value")
      ,$"timestamp")
      .select("parsed_value.machineNo",
            "parsed_value.termPaymentDay",
            "parsed_value.currency",
            "parsed_value.branchNo",
            "parsed_value.tellerID",
            "parsed_value.operTime",
            "parsed_value.cashNum",
            "parsed_value.faceValue",
            "timestamp"
          )

    //get_json_object只能取得一列
//    val parsed=stream.select(get_json_object(($"value").cast("string"),"$.machineNo"),$"timestamp")

//    val parsed=stream.select(get_json_object(($"value").cast("string"),"$.machineNo"),
//      get_json_object(($"value").cast("string"),"$.termPaymentDay"),
//      get_json_object(($"value").cast("string"),"$.currency"),
//      get_json_object(($"value").cast("string"),"$.branchNo"),
//      get_json_object(($"value").cast("string"),"$.tellerID"),
//      get_json_object(($"value").cast("string"),"$.operTime"),
//      get_json_object(($"value").cast("string"),"$.cashNum"),
//      get_json_object(($"value").cast("string"),"$.faceValue")
//    ,$"timestamp")


    val query=parsed.writeStream
      .trigger(ProcessingTime("5 seconds"))
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
