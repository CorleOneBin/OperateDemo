package cn.zhubin.function;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import com.amazonaws.services.datapipeline.model.EvaluateExpressionRequest;

public class TuoMin extends UDF{
      public Text evaluate(final Text s) {
    	  if(s == null) {
    		  return null;
    	  }
    	  String str = s.toString();
    	  str = str.substring(0,1) + "***" + str.substring(str.length()-1, str.length());
    	  return new Text(str);
      }
}
