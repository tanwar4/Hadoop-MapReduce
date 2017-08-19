package cs6240.neu.pagerank;

import java.io.IOException;
import java.util.TreeMap;
import java.util.logging.Logger;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNMapper extends Mapper<Object, Text, NullWritable, Text> {
	// Our output key and value Writables
	private TreeMap<Double, Text> repToRecordMap = new TreeMap<Double, Text>();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			// Parse the input string into a nice map

			String node = key.toString();
			String[] pageInfo = value.toString().split("\t") ;
			double pagerank = Double.parseDouble(pageInfo[1]);

			repToRecordMap.put(pagerank, new Text(value));

			if (repToRecordMap.size() > 100) {  //get top 100 result
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Text t : repToRecordMap.values()) {
				context.write(NullWritable.get(), t);
			}

        }
    
}

