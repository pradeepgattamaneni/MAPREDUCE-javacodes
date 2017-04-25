package h1bproject;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ApplicationsPerYear 
{

	 
	
	
	public static void main(String args[]) throws Exception
	
	{
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf,"Applications Per Year");
	
	job.setJarByClass(ApplicationsPerYear.class);
	job.setMapperClass(Mapper7.class);
	job.setCombinerClass(Reducer7.class);
	job.setReducerClass(Reducer7.class);
	
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(LongWritable.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(LongWritable.class);
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	
	System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
