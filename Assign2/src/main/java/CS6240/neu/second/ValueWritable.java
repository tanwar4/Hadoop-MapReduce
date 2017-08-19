package CS6240.neu.second;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


/**
 * class to store values (type ,temperature, year)
 * this value is passed to reducer
 */
public class ValueWritable implements Writable{

   
   
    private Text type;
    private IntWritable year;
    private FloatWritable  temp;
    
    
	public ValueWritable()
    {
        setType("");
        setYear(0);
        setTemp(0);
    }
    
    public ValueWritable(String s, float t, int y)
    {
    	setType(s);
    	setTemp(t);
    	setYear(y);
    }

    public String getType() {
		return type.toString();
	}

	public void setType(String type) {
		this.type = new Text(type);
	}

	public int getYear() {
		return year.get();
	}

	public void setYear(int year) {
		this.year = new IntWritable(year);
	}
    public float getTemp() {
		return temp.get();
	}

	public void setTemp(float temp) {
		this.temp = new FloatWritable(temp);
	}


	public void write(DataOutput d) throws IOException {
		type.write(d);
		temp.write(d);
		year.write(d);      
    }

    public void readFields(DataInput di) throws IOException {
		type.readFields(di);
		temp.readFields(di);
		year.readFields(di); 
    }

 
    
}
