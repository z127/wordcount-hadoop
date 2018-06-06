import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by zqj on 2018/4/24.
 */
public class PaodingDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        //System.setProperty("hadoop.home.dir", "G:\\hadoopdir\\hadoop-2.7.2\\hadoop-2.7.2");
        Configuration conf=new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 2){

            System.err.println("用法：wordcount <int> [<in>...] <out>");
            System.exit(2);

        }
        //设置一个分片的最大大小
        conf.setLong("mapreduce.input.fileinputformat.split.maxsize",4000000);
        //String basepath="H:\\baodingfenci\\https___download.csdn.net_download_qq1010885678_9447741\\data";
       // String basepath1="H:\\baodingfenci\\data";
        String inPath =otherArgs[0]  ;
        String outPath =  otherArgs[1];
        //设置自定义的PaodingInputFormat
        JobInitModel job = new JobInitModel(new String[]{inPath + "/MP3", inPath + "/camera", inPath + "/computer"
                , inPath + "/household", inPath + "/mobile"}, outPath, conf, null, "paoding-special", PaodingDriver.class
                , PaodingInputFormat.class, PaodingMapper.class, Text.class, Text.class, null, null
                , PaodingReducer.class, Text.class, Text.class);
        BaseDriver.initJob(new JobInitModel[]{job});
    }
}
