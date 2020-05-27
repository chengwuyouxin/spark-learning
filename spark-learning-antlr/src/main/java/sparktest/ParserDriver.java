package sparktest;


import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;

import java.io.IOException;

public class ParserDriver {
    public static void main(String[] args) throws IOException {
//        String query = "select name,id from user";
//        SqlBaseLexer lexer = new SqlBaseLexer(new ANTLRInputStream(query));
//        SqlBaseParser parser = new SqlBaseParser(new CommonTokenStream(lexer));
//        String query = "select name from user";

//        //构造词法分析器 lexer，词法分析的作用是产生记号
//        SqlBaseLexer lexer = new SqlBaseLexer(new ANTLRInputStream(query));
//
//        //用词法分析器 lexer 构造一个记号流 tokens
//        CommonTokenStream tokens = new CommonTokenStream(lexer);
//
//        //再使用 tokens 构造语法分析器 parser,至此已经完成词法分析和语法分析的准备工作
//        SqlBaseParser parser = new SqlBaseParser(tokens);
//
//        MySparkVisitor visitor = new MySparkVisitor();
//
//        String res = visitor.visitSingleStatement(parser.singleStatement());
//
//        System.out.println("res="+res);

        String query = "SELECT name FROM student where age > 18";
        SqlBaseLexer lexer = new SqlBaseLexer(new ANTLRInputStream(query.toUpperCase()));
        SqlBaseParser parser = new SqlBaseParser(new CommonTokenStream(lexer));
        MySparkVisitor visitor = new MySparkVisitor();
        SqlBaseParser.SingleStatementContext ssc = parser.singleStatement();
        String res = visitor.visitSingleStatement(ssc);
        System.out.println("res="+res);

    }
}
