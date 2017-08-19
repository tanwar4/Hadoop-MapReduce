package cs6240.neu.pagerank;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNReducer extends
		Reducer<NullWritable, Text, NullWritable, Text> {

	private TreeMap<Double, Text> repToRecordMap = new TreeMap<Double, Text>();// structure which hold the data in key sorted format 

	@Override
	public void reduce(NullWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			String[] pageInfo = value.toString().split("\t") ;  //adjacency List
			double pagerank = Double.parseDouble(pageInfo[1]);  //page rank

			repToRecordMap.put(pagerank,new Text(value));  //page rank used as key hence the data will be sorted according to rank

			if (repToRecordMap.size() > 100) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}

		for (Text t : repToRecordMap.descendingMap().values()) {
		    String tokens[] = t.toString().split("\t");
		    String output = tokens[1]+"\t"+ tokens[0]; 
			context.write(NullWritable.get(), new Text(output));
		}
	}
}

