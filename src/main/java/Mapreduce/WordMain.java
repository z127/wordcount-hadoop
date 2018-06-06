package Mapreduce;

/**
 * Created by zqj on 2018/5/12.
 */


        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.IntWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
        import org.apache.hadoop.util.GenericOptionsParser;

public class WordMain {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        Job job = new Job(conf, "word count");
        job.setJarByClass(WordMain.class);
        job.setMapperClass(WordMapper.class);
        job.setCombinerClass(WordReducer.class);
        job.setReducerClass(WordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("H:\\\\baodingfenci\\\\123.txt "));
        FileOutputFormat.setOutputPath(job, new Path("H:\\\\baodingfenci\\\\output.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}