package cs6240.neu.pagerank;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
	public static final double ALPHA = 0.85;
	private int numberOfNodes;
	private double sinkMass;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		numberOfNodes =Integer.parseInt(context.getConfiguration().get("TotalNodes")); //retrieve values from context
		sinkMass= Double.parseDouble(context.getConfiguration().get("TotalSinkMass"));
	}

	private Text outValue = new Text();

	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		double summedPageRanks = 0;
		Node originalNode = new Node();

		for (Text textValue : values) {

			Node node = Node.getNode(textValue.toString(),0.0);  //get a node data structure

			if (node.containsAdjacentNodes()||node.isSinkNode()) {  //Node received-- Retrieve the graph structure
				originalNode = node;
			} else {
				//page rank  contribution received
				summedPageRanks += node.getPageRank();
			}

		}

		double dampingFactor = ( ALPHA/ (double) numberOfNodes);
		double newPageRank = dampingFactor + ((1.0 - ALPHA) * summedPageRanks)+ ((1.0 - ALPHA)*(sinkMass/numberOfNodes));
		originalNode.setPageRank(newPageRank);

		outValue.set(originalNode.toString());		
		context.write(key, outValue);
	}
}
