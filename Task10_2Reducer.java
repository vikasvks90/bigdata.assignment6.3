/**
 * <h1>Task10_2Reducer</h1>
 * Reducer program to calculate the total units sold for each Company
 * This class will take input as (Key,Value) pair from output of mapper class
 * value will be a combined list for all the values for a given key
 * */
package mapreduce.assignment6.task10;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task10_2Reducer extends Reducer<Text, StateSales, Text, StateSales>{	
	ArrayList<StateSales> stateSalesData = new ArrayList<StateSales>();
	
	public void reduce(Text key, Iterable<StateSales> values,Context context) throws IOException, InterruptedException{
		stateSalesData.clear();
		
		for (StateSales value : values) {
			//value is a single object containing different instances in different passes.
			//So, create new StateSales objects every time we need to store it to the ArrayList
			stateSalesData.add(new StateSales(value.getState(), value.getSales()));
		}
		
		Collections.sort(stateSalesData);
		
		int counter = 1;
		for (StateSales statesales : stateSalesData) {
			//calculating the top 3 state-wise sales for each company
			if (counter > 3) {
				break;
			}
			context.write(key, statesales);
			counter++;
		}		
	}
}