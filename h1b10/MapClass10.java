package h1b10;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;

public  class MapClass10 extends Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		String[] parts = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");//From statckoverflow.com
		String status = parts[1];
		String job = parts[4].replaceAll("\"", "");
		context.write(new Text(job), new Text(status));
	}
}
