import org.apache.spark.sql.SparkSession

object Ex3_DstreamJoinDstream {
  def main(args:Array[String]): Unit ={
    val spark=SparkSession.builder()
      .appName("Ex3_DstreamJoinDstream")
      .getOrCreate()

    import spark.implicits._

    val orglevelStream=spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","dn01:9092,dn02:9092,dn03:9092")
      .option("subscribe","orglevel")
      .option("startingOffsets","earliest")
      .option("endingOffsets","latest")
      .load()

    val orglevelDF=orglevelStream.selectExpr("CAST(value AS STRING)","timestamp")
      .withColumnRenamed("timestamp","orgleveltime")
      .as[(String,Long)]
//    .withWatermark("orgleveltime","10 Seconds")
      .map(record => record._1.split("\\|"))
      .map(record => OrgLevel(record(0), record(1), record(2),
        record(3), record(4), record(5),
        record(6), record(7), record(8))).toDF()


    val bsmstranslogStream=spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","dn01:9092,dn02:9092,dn03:9092")
      .option("subscribe","bsmstranslog")
      .option("startingOffsets","earliest")
      .option("endingOffsets","latest")
      .load()

    val bsmsTransLogDF=bsmstranslogStream
      .selectExpr("CAST(value AS STRING)","timestamp").as[(String,Long)]
//      .withWatermark("timestamp","10 Seconds")
      .map(record => record._1.split("\\|"))
      .map(record => BsmsTrans(record(0), record(1), record(2),
        record(3), record(4), record(5),
        record(6), record(7), record(8),
        record(9), record(10), record(11),
        record(12), record(13), record(14),
        record(15), record(16), record(17),
        record(18), record(19), record(20),
        record(21), record(22), record(23),
        record(24), record(25), record(26),
        record(27), record(28), record(29),
        record(30), record(31), record(32),
        record(33), record(34), record(35),
        record(36), record(37), record(38))).toDF()


    val res=orglevelDF.join(bsmsTransLogDF,"BranchId")
      .groupBy(orglevelDF("BranchID"))
      .count()

    val query=res.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }
}
case class BsmsTrans(IncFlg:String,ModifiedDate:String,ProvBankCode:String,
                     TransactionJournalID:String,TransactionNo:String,TellerID:String,
                     BranchID:String,TransactionGroupID:String,TransactionGroupName:String,
                     ModifiedTime:String,ProductNoForOcrm:String,
                     Currency:String,TranMarketRelationID:String,
                     ServiceID:String,OutAmount:String,OutCurrency:String,
                     ProductSubType:String,flag1:String,flag2:String,flag3:String,
                     flag4:String,flag5:String,EntryTransactionNo:String,
                     ISCALL:String,TICKETNUMBER:String,TAKETYPE :String,
                     CUSTTYPE:String,CUSTLVL :String,TRANTOPID:String,
                     TRANTYPEID:String,ARRIVETIME:String,BOOKWAY:String,
                     WINID:String,ISARRIVE :String,
                     TOWINID :String,Appraisal:String,UnsatisfactoryCode:String,
                     TRANKIND:String,Day:String)

case class OrgLevel(ProvBankCode:String,ProvBankName:String,SndBranchID:String,
                    SndBranchName:String,AdmBranchID:String,AdmBranchName:String,
                    BranchID:String,BranchName:String,day:String)