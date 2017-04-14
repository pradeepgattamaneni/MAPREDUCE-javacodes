package myWordCount;


import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyWordCount {
	
	public static class MyWordCountMap extends Mapper < Text, Text, Text, LongWritable > {
        static String wordToSearch;
        private final static LongWritable ONE = new LongWritable(1L);
        private Text word = new Text();
        public void map(Text key, Text value, Context context)
        throws IOException, InterruptedException {
            System.out.println(wordToSearch); // Here the value is coming as Null
            if (value.toString().compareTo(wordToSearch) == 0) {
                context.write(word, ONE);
            }
        }
    }


    public static class SumReduce extends Reducer < Text, LongWritable, Text, LongWritable > {

        public void reduce(Text key, Iterator < LongWritable > values,
            Context context) throws IOException, InterruptedException {
            long sum = 0L;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] rawArgs) throws Exception {

        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "wordcount");
        job.setJarByClass(MyWordCountMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapperClass(MyWordCountMap.class);
        job.setReducerClass(SumReduce.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        String MyWord = args[2];
        MyWordCountMap.wordToSearch = MyWord;
        job.waitForCompletion(true);
    }


}
