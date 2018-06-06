import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by zqj on 2018/5/12.
 */
public class MyWordCount {
    public  static class TokenizerMapper extends Mapper<Object,Text,Text,IntWritable> {
        private final static IntWritable one=new IntWritable(1);
        private final static IntWritable two=new IntWritable(2);
        private org.apache.hadoop.conf.Configuration conf;
        private Set<String> patternsToDouble=new HashSet<String>();



        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
           String[] list=conf.getStrings("worddouble",null);
           for(String name:list)
           {
               System.out.println("list"+ name);
               patternsToDouble.add(name);
           }

        }

        @Override
        public  void map(Object key, Text value, Context context) throws IOException,InterruptedException {
            String line=value.toString();
         String[] words=line.split(" ");
            System.out.println("读取word");
         for(String word: words)
         {
            if(patternsToDouble.contains(word))
            {
                System.out.println("contains");
                context.write(new Text(word),two);
            }else {
                context.write(new Text(word),one);
            }
             System.out.println(word);
         }

        }




    }

    public  static class  IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        private IntWritable result=new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context arg2) throws IOException, InterruptedException {
                int sum=0;
                for(IntWritable val:values)
                {
               /* if(val.get()==2)
                {
                    System.out.println("2");;
                }*/
                    sum+=val.get();

                }
                result.set(sum);
                System.out.println(key+" "+result);
                arg2.write(key,result);
            }

    }





    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
        if(otherArgs.length<2)
        {
            System.out.println("error wrong");
            System.exit(2);
        }

        Job job=Job.getInstance(conf,"zhouyangwordcount");
        //job.setInputFormatClass(TextInputFormat.class);
        job.getConfiguration().setStrings("worddouble","zhou","yang");
        job.setJarByClass(MyWordCount.class);
        job.setMapperClass(TokenizerMapper.class);
       //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        String     inputPath=otherArgs[0];
        String     outputPath=otherArgs[1];
        System.out.println("inputpath"+inputPath);
        System.out.println("outputpath"+outputPath);
        FileInputFormat.setInputPaths(job,new Path(inputPath));
        FileOutputFormat.setOutputPath(job,new Path(outputPath));
        System.exit(job.waitForCompletion(true)?0:1);

    }
}
