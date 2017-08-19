package cs6240.neu.pagerank;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * This reducer class performs following functions:
 * Calculate total no of nodes in the graph
 * Emits the adjacency list which has been modified to accommodate the second attribute as page rank(which is initially set as 0.0)
 * */

public class PreProcessingReducer extends Reducer<Text, Text, Text, Text> {
	
	//Global counter for calculating total no of nodes in the graph
	public static enum Counter{
		NODE_COUNT
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{		
		for( Text value: values){
			context.write(key, value);
		}		
		context.getCounter(Counter.NODE_COUNT).increment(1); //incrementing the global node counts for each unique node encountered
	}

	
}
