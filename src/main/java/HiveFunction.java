/**
 * Created by zqj on 2018/5/10.
 */
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class HiveFunction extends  UDF{
    public Text evaluate(Text str) {
        return this.evaluate(str, new IntWritable(0));
    }



    public Text evaluate(Text str, IntWritable flag) {

        // 如果不等于null，就进行转换大小写的操作，说明数据合法
        if (str != null) {
            // 如果等于0，表示转换小写
            if (flag.get() == 0) {
                return new Text(str.toString().toLowerCase());
                // 如果等于1，表示转换大写
            } else if (flag.get() == 1) {
                return new Text(str.toString().toUpperCase());
            } else
                return null;
        } else
            return null;
    }


    public static void main(String[] args) {
        System.out.println(new HiveFunction().evaluate(new Text("Hadoop"), new IntWritable(0)));
    }



}
