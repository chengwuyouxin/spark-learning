package util

import java.sql.{Connection, DriverManager, Statement}

import org.apache.spark.sql.{ForeachWriter, Row}

class MysqlSink  extends ForeachWriter[Row]{
  var conn:Connection=_
  var st:Statement=_
  val url="jdbc:mysql://22.11.97.142:3306/cash"
  val user="root"
  val password="admin"

  override def open(partitionId: Long, version: Long): Boolean = {
    Class.forName("com.mysql.jdbc.Driver")
    conn=DriverManager.getConnection(url,user,password)
    st=conn.createStatement()
    true
  }

  override def process(value: Row): Unit = {
//    st.executeUpdate(s"insert into cash.wordcount values (" +
//      s"'${value.getAs[String]("value")}'," +
//      s"'${value.getAs[Int]("count")}')")

    st.executeUpdate(s"insert into cash.wordcount2 values (" +
          s"'${value.getAs[String]("timestamp")}'," +
          s"'${value.getAs[Int]("value")}',1)")

//    st.executeUpdate(s"insert into cash.wordcount values ('${value.getAs[String]("value")}',1)")
  }

  override def close(errorOrNull: Throwable): Unit = {
    conn.close()
  }
}
