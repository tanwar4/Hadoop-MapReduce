package CS6240.neu.second;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<CompositeKeyWritable, ValueWritable, Text, Text> {

	/**
	 *
	 * @param key
	 * @param values
	 * @param context
	 * 
	 * The key is Composite Key which also contains station Id 
	 * The values received in reduce function call is a list for station Id (present in key)
	 *   which is sorted by year and temperature type
	 */
	@Override
	public void reduce(CompositeKeyWritable key, Iterable<ValueWritable> values, Context context)
	{
		String stId = key.getStationId();

		//initialize variables
		float sum_max=0,sum_min=0;
		int count_max=0,count_min=0;
		String st ="";
		int key_year=0;

		for(ValueWritable vw: values){//iterating through the values  for accumulating same year and temp_type data

			//if new year different than previous year  calculate the avg from accumulated data  and reset the variables
			//value are already sorted by secondary sort , so we just need to check for change in year
			if(vw.getYear() != key_year && key_year!=0){
				float min_avg = (count_min== 0)? -9999: sum_min/count_min;  //calculating avg of old year
				float max_avg = (count_max== 0)? -9999: sum_max/count_max;
				st = st+" ("+key_year+", "+min_avg+", "+max_avg+") ";

				sum_max=0;count_max=0;  //resetting  variables
				sum_min=0;count_min=0;
			}
			if(vw.getType().equalsIgnoreCase("TMIN")){  //checking type and updating the variables of old year values
				sum_min= sum_min+(float)vw.getTemp();
				count_min +=1;
			}
			else{
				sum_max= sum_max+(float)vw.getTemp(); 
				count_max +=1;
			}
            key_year = vw.getYear();  //get next yeat from list
		}
		
		//condition to handle last year data since the pointer already moved forward
		float min_avg = (count_min== 0)? -9999: sum_min/count_min;
		float max_avg = (count_max== 0)? -9999: sum_max/count_max;
		st = st+" ("+key_year+", "+min_avg+", "+max_avg+") ";  //final data for station id

		try {
			context.write(new Text(stId),new Text(st));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	

}
