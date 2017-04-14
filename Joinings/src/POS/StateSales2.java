package POS;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class StateSales2 {
	public static class StateMapClass extends Mapper<LongWritable,Text,Text,IntWritable>{
		protected void map(LongWritable key, Text value, Context context)
		{
			try{
				String[] str = value.toString().split(",");
				int qty=Integer.parseInt(str[2]);
				int price = Integer.parseInt(str[3]);
				int amount=qty*price;
				
				
			    
			    context.write(new Text(str[4]),new IntWritable(amount));
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
		
	}
	
	public static class StateReduceClass extends Reducer<Text,IntWritable,Text,IntWritable>{
		
				
		public void reduce(Text key,Iterable <IntWritable> values,Context context)throws IOException, InterruptedException
		{
			int sum=0;
			for(IntWritable val:values)
			sum=sum + val.get();
			context.write(key, new IntWritable(sum));
		}
		
		
	}
	public static void main(String[] args) 
            throws IOException, ClassNotFoundException, InterruptedException {

Configuration conf = new Configuration();
conf.set("mapreduce.output.textoutputformat.separator", ",");
Job job = Job.getInstance(conf);
job.setJarByClass(StateSales2.class);
job.setJobName("State Wise Sales");
job.setMapperClass(StateMapClass.class);
job.setCombinerClass(StateReduceClass.class);
job.setReducerClass(StateReduceClass.class);
//job.setNumReduceTasks(0);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(IntWritable.class);

FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));

job.waitForCompletion(true);


}

}
