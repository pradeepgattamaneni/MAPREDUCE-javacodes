package sexRatio;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.SequenceFileInputFormat;
//import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SexRatio {
	

	public static class MapClass extends Mapper<LongWritable,Text,NullWritable,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			
			String row = value.toString();
        	String[] tokens = row.trim().split(",");
        	String age[] = tokens[0].trim().split(":");
            String age1=age[1].trim();
            
        	String gender[] = tokens[3].trim().split(":");
        	String gender1= gender[1].trim();
        	String output = age1+","+gender1;
      	  	context.write(NullWritable.get(),new Text(output));
        }  
		}
	public static class PartPartitioner extends Partitioner < NullWritable, Text >
	   {
	      public int getPartition(NullWritable key, Text value, int numReduceTasks)
	      {
					String parts[] = value.toString().trim().split(",");
	         int age =Integer.parseInt(parts[0]); ;
	    	  
	         
	         if(age<=12) return 0;
	         else if(age>12 && age<=17) return 1;
	         else if(age>18 && age<=40) return 2;
	         else if(age>40 && age<=60) return 3;
	         else if(age>60 && age<=80) return 4;
	         else  return 5;
	         
	      }
	   }
	
	public static class ReduceClass extends Reducer<NullWritable ,Text,NullWritable,Text>{
		long malecount = 0;
		long femalecount = 0;
		public void reduce(NullWritable key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
			
			for(Text val : value){
				String parts[] = val.toString().trim().split(",");
				if(parts[1].trim().equals("\" Female\""))femalecount++;
				else if (parts[1].trim().equals("\" Male\""))malecount++;
			}
//			long temp1=malecount;
//			long temp2=femalecount;
//			while(malecount!=femalecount)
//			{
//				if(malecount>femalecount)
//					malecount=malecount-femalecount;
//					else femalecount=femalecount-malecount;
//				}
			
			String output = "Sex Ratio by age group is (male:female) ; " +malecount+":"+femalecount;
			context.write(NullWritable.get(),new Text(output));
				
			}
			
			
			
			
		}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Sex Ratio by Age Group");
		job.setJarByClass(SexRatio.class);
		job.setMapperClass(MapClass.class);
		job.setPartitionerClass(PartPartitioner.class);
		job.setReducerClass(ReduceClass.class);
		
		job.setNumReduceTasks(6);
		//job.setPartitionerClass(PartPartitioner.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		//job.setInputFormatClass(SequenceFileInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
