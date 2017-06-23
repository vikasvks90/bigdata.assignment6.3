/**
 * <h1>Task10_2Mapper</h1>
 * Mapper program to calculate the total units sold for each Company
 * This class will take input as (Key,Value) pair from a given file and will
 * produce the output as (Key,Value) pair.
 * */
package mapreduce.assignment6.task10;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Task10_2Mapper extends Mapper<LongWritable, Text, Text, StateSales> {
	Text outKey = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\t");
		outKey.set(lineArray[0]);
		context.write(outKey, new StateSales(lineArray[1], Integer.parseInt(lineArray[2])));
	}
}