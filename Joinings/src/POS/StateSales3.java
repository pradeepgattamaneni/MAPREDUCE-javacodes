package POS;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class StateSales3 {
	public static class ProfitMapClass extends Mapper<LongWritable,Text,Text,IntWritable>{
		protected void map(LongWritable key, Text value, Context context)
		{
			try{
				String[] str = value.toString().split(",");
				
				
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
	}
		

}
