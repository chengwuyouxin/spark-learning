package com.lpq.spark.core.listener

class DcTable(val database:String,val table:String){

  override def toString: String={
    return database+"."+table;
  }

}
