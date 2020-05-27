package com.lpq.spark.sql.lineage.table

/**
  * @author liupengqiang
  * @date 2020/5/12
  */
class MyTable (val database:String, val table:String){
  override def toString: String = {
    database+"."+table;
  }
}
