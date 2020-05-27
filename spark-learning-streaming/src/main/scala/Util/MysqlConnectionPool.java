package Util;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

public class MysqlConnectionPool implements Serializable {
    private static String jdbcDriver="com.mysql.jdbc.Driver";
    private static  String url="jdbc:mysql://22.11.97.142:3306/cash";
    private static  String user="root";
    private static  String password="admin";

    private static  int initConn=10;
    private static  int incrementalConn=5;
    private static  int maxConn=1000;
    private static  int curConn=0;
    private static  LinkedList<Connection> queue=new LinkedList<Connection>();

//    public MysqlConnectionPool(){
//        initPool();
//    }
//
//    private void initPool(){
//        if(queue != null){
//            return;
//        }
//        try {
//            Class.forName(jdbcDriver);
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//        queue=new LinkedList<Connection>();
//        for(int i=0;i<initConn;i++){
//            if(this.maxConn>0 && this.queue.size()>=this.maxConn){
//                break;
//            }
//            try {
//                queue.offer(createConnection());
//                curConn++;
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
//    }

    public static synchronized Connection getConnection() throws SQLException {
        if(queue==null){
            return null;
        }
        Connection conn=getFreeConnection();
        while(conn==null){
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            conn=getFreeConnection();
        }
        return conn;
    }

    public static void returnConnection(Connection conn){
        if(conn==null){
            return;
        }
        queue.offer(conn);
    }

    public static synchronized void closeConnectionPool() throws SQLException {
        if(queue==null)
            return;
        Connection conn=null;
        while(!queue.isEmpty()){
            conn=queue.poll();
            if(conn!=null){
                conn.close();
            }
        }
        queue=null;
    }

    private static Connection createConnection() throws SQLException {
        Connection conn=DriverManager.getConnection(url,user,password);
        //第一次创建连接时候检查数据库允许的最大连接数
        if(queue.size()==0){
            DatabaseMetaData metaData=conn.getMetaData();
            int diverMaxConnections=metaData.getMaxConnections();
            if(diverMaxConnections>0 && diverMaxConnections>maxConn){
                maxConn=diverMaxConnections;
            }
        }
        return conn;
    }

    private static Connection getFreeConnection() throws SQLException {
        Connection conn=queue.poll();
        if(conn==null){
            if(curConn<maxConn){
                for(int i=0;i<=incrementalConn;i++){
                    queue.offer(createConnection());
                    curConn++;
                }
                conn=queue.poll();
            }
        }
        return conn;
    }
}
