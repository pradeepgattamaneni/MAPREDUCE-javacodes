package h1b10;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReduceClass10 extends Reducer <Text,Text,NullWritable,Text>{
	private TreeMap<Double,String> top10 = new TreeMap<>();
	public void reduce(Text key,Iterable<Text> value,Context context){
		double total = 0;
		double sucess = 0;
		for (Text val:value){
			String status = val.toString();
			if(status.equals("\"CERTIFIED\"") || status.equals("\"CERTIFIED-WITHDRAWN\"")){
				total++;
				sucess++;
			}
			else total++;
		}
		System.out.printf("%f,%f",sucess,total);
		double sucess_rate = (sucess/total)*100;
		if (sucess_rate >= 70 && total >=1000){
			String output = key.toString() +"@"+String.format("%.0f",total)+"@"+String.format("%.2f ",sucess_rate);
			top10.put(sucess_rate, output);				
		}
	}
	protected void cleanup(Context context) throws IOException, InterruptedException{
		for(String val : top10.descendingMap().values()){
			context.write(NullWritable.get(),new Text(val));
		}
	}//
}
