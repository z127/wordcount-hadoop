/**
 * Created by zqj on 2018/4/24.
 */
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/29.
 */
public class PaodingReducer extends Reducer<Text, Text, Text, Text> {
    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder totalStr = new StringBuilder();
        for (Text value : values) {
            totalStr.append(value.toString()).append(" ");
        }
        System.out.println("reducer key: "+ key);
        System.out.println("reducer value: "+ totalStr.toString());
        v.set(totalStr.toString());
        Text m=new Text(key);
        context.write(m, v);
    }
}