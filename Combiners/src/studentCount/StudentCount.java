package studentCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StudentCount {

	public static class Mapper_Class extends Mapper<LongWritable,Text,Text,IntWritable>{
		private final IntWritable one=new IntWritable(); 
		public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException
	{
			String[] rec=value.toString().split(",");
			String city=rec[3];
			one.set(Integer.parseInt(rec[2]));
			con.write(new Text(city),one);
	}
	}
public static class Reducer_Class1 extends Reducer<Text,IntWritable, Text ,IntWritable>{
	IntWritable maxValue=new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context con)throws IOException, InterruptedException
	{
		int sum=0;
			
		for(IntWritable value:values)
		{
			sum+=value.get();
		
		}
		maxValue.set(sum);
		con.write(key, maxValue);		
	}
}
	public static void main(String[] args)throws Exception
	{		
		Configuration conf=new Configuration();
		//conf.set("mapreduce.output.textoutputformat.separator",",");
		Job job=Job.getInstance(conf,"");
		job.setJarByClass(StudentCount.class);		
		
		job.setJobName("Combiners ");
		
		job.setMapperClass(Mapper_Class.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(Reducer_Class1.class);
		job.setCombinerClass(Reducer_Class1.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);// false- X job compilation 			
	}	
}