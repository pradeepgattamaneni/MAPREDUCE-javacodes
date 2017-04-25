package h1bproject;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper7 extends Mapper<LongWritable,Text,Text,LongWritable>
{
	
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {  	
  	  if(key.get()> 0 )

  	  {
  		
  	String [] token=value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"); 
		
  	  context.write(new Text(token[7]),new LongWritable(1));
  	  }
  	  }
    }
