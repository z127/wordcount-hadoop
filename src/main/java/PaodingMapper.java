import net.paoding.analysis.analyzer.PaodingAnalyzer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.io.StringReader;

/**
 * Created by zqj on 2018/4/24.
 */
public class PaodingMapper extends Mapper<Text, Text, Text, Text> {
    Text k = new Text();
    Text v = new Text();

    //庖丁分词器
    PaodingAnalyzer analyzer = new PaodingAnalyzer();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //对读取的行进行分词并拼接
        StringBuilder totalStr = new StringBuilder();
        //对文件内容进行分词
        TokenStream tokenStream = analyzer.tokenStream("", new StringReader(value.toString()));
        while (tokenStream.incrementToken()) {
            CharTermAttribute attribute = tokenStream.getAttribute(CharTermAttribute.class);
            totalStr.append(attribute.toString()).append(" ");
        }




        v.set(totalStr.toString());
        //System.out.println("Mapper key: "+ key);
       // System.out.println("Mapper value: "+ v.getBytes().toString());
        context.write(key, v);
    }

}