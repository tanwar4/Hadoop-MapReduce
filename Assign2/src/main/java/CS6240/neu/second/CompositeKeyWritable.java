package CS6240.neu.second;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils; 

/*
 * Composite key  to sort data based on  Station Id , year , and type 
 */
public class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable> {

   
    private String stationId;
    private String type;
    private int year;
    
    public CompositeKeyWritable()
    {
        
    }
    
    public CompositeKeyWritable(String s, String t, int y)
    {
        this.stationId =s;
        this.type= t;
        this.year=y;
    }

    public String getStationId() {
        return stationId;
    }

    public void setStationId(String stationId) {
        this.stationId = stationId;
    }

    public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public void write(DataOutput d) throws IOException {
        WritableUtils.writeString(d, stationId);
        WritableUtils.writeString(d, type);
        WritableUtils.writeVInt(d, year);
    }

    public void readFields(DataInput di) throws IOException {
        stationId = WritableUtils.readString(di);
        type= WritableUtils.readString(di);
        year = WritableUtils.readVInt(di);
    }

 
    public int compareTo(CompositeKeyWritable o) {
		int stId = stationId.compareTo(o.stationId);
		if (stId != 0) {
			return stId;
		} else {			
			int yrcmp = Integer.compare(year,o.year);
			if (yrcmp != 0) {
				return yrcmp;
			} else {
				return -1*type.compareTo(o.type);
			}
		}
    }
    
    public String toString()
    {
        return (new StringBuilder().append(stationId).append("\t").append(type).append("\t").append(year).toString());
    }
    
}
