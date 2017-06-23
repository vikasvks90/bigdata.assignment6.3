/**
 * <h1>StateSales</h1>
 * This class is implementing WritableComparable interface to give reducer 
 * input key in sorted manner based on company name and tv size
 * */
package mapreduce.assignment6.task10;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
 
public class StateSales implements WritableComparable<StateSales> {
 
    private String state;
    private int sales;
 
    public StateSales() {
    }

    public StateSales(String state, int sales) {
    	set(state, sales);
    }
    
    public String getState() {
        return state;
    }
 
    public int getSales() {
        return sales;
    }
 
    public void set(String state, int sales) {
        this.state = state;
        this.sales = sales;
    }
 
    @Override
    // This method is inherited from Writable interface and will be used to deserialize the fields of the object from ‘in’.
    public void readFields(DataInput in) throws IOException {
    	state = in.readUTF();
    	sales = in.readInt();
    }
 
    @Override
    // This method is inherited from Writable interface and will be used to serialize the fields of the object to ‘out’.
    public void write(DataOutput out) throws IOException {
    	out.writeUTF(state);
    	out.writeInt(sales);
    }
 
    @Override
    //this method is inherited from Object class and is used to print user defined objects accordingly
    public String toString() {
        return state + "\t" + sales;
    }
 
    @Override
    // This method is inherited from Comparable interface and will be used to sort the input keys to reducer
    //based on hashcode and equals method
    public int compareTo(StateSales ss) {
        return (-1) * (sales - ss.getSales());
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof StateSales){
        	StateSales ss = (StateSales) o;
            return state.equalsIgnoreCase(ss.getState()) && (sales == ss.getSales());
        }
        return false;
    }
  
}