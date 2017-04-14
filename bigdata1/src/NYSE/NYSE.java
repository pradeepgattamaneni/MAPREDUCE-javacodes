package nyse;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class NYSE {
	public static class NewyorkMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
		
		//Text stock_sym = new Text();
		//FloatWritable percent = new FloatWritable();

	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			String[] record = value.toString().split(",");
			
			String stockSymbol = record[1];
			float highVal = Float.valueOf(record[4]);
			float lowVal = Float.valueOf(record[5]);
			float percentChange = ((highVal - lowVal) * 100) / lowVal;
			//stock_sym.set(stockSymbol);
			//percent.set(percentChange);
			//context.write(stock_sym, percent);
			context.write(new Text(stockSymbol), new FloatWritable(percentChange));
			
		} catch (IndexOutOfBoundsException e) {
		} catch (ArithmeticException e1) {
		}
	}
}
	
	
	
	public static class NewyorkReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		FloatWritable maxValue = new FloatWritable();
		
		
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			float maxPercentValue=0;
			float temp_val=0;
			for (FloatWritable value : values) {
				temp_val = value.get();
				if (temp_val > maxPercentValue) {
					maxPercentValue = temp_val;
				}
			}
			maxValue.set(maxPercentValue);
			context.write(key, maxValue);
		}
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out
					.printf("Usage: StockPercentChangeDriver <input dir> <output dir>\n");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(NYSE.class);
		job.setJobName(" just mapping marks with id");
		job.setMapperClass(NewyorkMapper.class);
		job.setReducerClass(NewyorkReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
}

}
