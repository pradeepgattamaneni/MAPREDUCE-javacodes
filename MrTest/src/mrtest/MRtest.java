package mrtest;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import junit.framework.TestCase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mrunit.mapreduce.*;
import org.junit.Test;

public class MRtest extends TestCase {
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;	
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable> mapReduceDriver;
	  
	public static class maptest extends Mapper<LongWritable, Text, Text, IntWritable>{
		Text stock_symbol=new Text();
		
		public void map(LongWritable key,Text value,Context ct)throws IOException,InterruptedException
		{
			String[] line=value.toString().split(",");
			int val=Integer.parseInt(line[1]);
			stock_symbol.set(line[0]);
			ct.write(stock_symbol,new IntWritable(val));
			
		}
	}

	
	public static class reducetest extends Reducer<Text, IntWritable, Text, DoubleWritable>{

		public void reduce(Text key,Iterable<IntWritable> values,Context ct)throws IOException,InterruptedException
		{
	      int sum = 0;
		  int count = 0;	
	         for (IntWritable val : values)
	         {       	
	        	sum += val.get();
	        	count ++;
	         }
	         
	        double avg = (double) sum / (double) count; 
			
	        ct.write(key,new DoubleWritable(avg));
			
		}
	}
	
	 
		 
public void setUp() {
//mapDriver = MapDriver.newMapDriver( new maptest());
//reduceDriver = ReduceDriver.newReduceDriver(new reducetest());
mapReduceDriver = MapReduceDriver.newMapReduceDriver(new maptest(), new reducetest());
}

@Test
public void testMapper() {
try {
	mapDriver.withInput(new LongWritable(), new Text("AEA,205"))
			.withInput(new LongWritable(), new Text("AEA,195"))	
	        .withOutput(new Text("AEA"), new IntWritable(205))
	        .withOutput(new Text("AEA"), new IntWritable(195))
	        .runTest();
	} 
catch (IOException e) {
    System.out.println(e.getMessage());
	//e.printStackTrace();
	}
}

@Test
public void testReducer() {
  try {
	  List<IntWritable> values = new ArrayList<IntWritable>();
	  values.add(new IntWritable(205));
	  values.add(new IntWritable(194));
	  reduceDriver.withInput(new Text("AEA"), values);
	  reduceDriver.withOutput(new Text("AEA"), new DoubleWritable(199.50));
	  reduceDriver.runTest();
  }
   catch (IOException e) {
       	System.out.println(e.getMessage());
		//e.printStackTrace();
		}
		} 


@Test
public void testMapReduce() {
  try {
	  mapReduceDriver.withInput(new LongWritable(), new Text("AEA,205"));
	  mapReduceDriver.withInput(new LongWritable(), new Text("AEA,194"));
	  mapReduceDriver.withOutput(new Text("AEA"), new DoubleWritable(199.5));
	  mapReduceDriver.runTest();
  		}
   catch (IOException e) {
      	System.out.println(e.getMessage());
		//e.printStackTrace();
	}
	}

}