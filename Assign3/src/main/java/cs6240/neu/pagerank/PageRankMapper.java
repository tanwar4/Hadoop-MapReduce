package cs6240.neu.pagerank;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Text, Text, Text> {
	
    private int totalNodes;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		totalNodes = Integer.parseInt(context.getConfiguration().get("TotalNodes"));
	}
	public static enum Counter{   //Global counter for Sink Node Mass
		SINK_NODE_MASS_COUNT
	}
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
        		
        	   // appending a special char to identify the type of node. This statement sends the graph structure to reducer
        	    if(value.toString().split("\t").length == 1)
        	    	context.write(key, new Text(value+"\t"+"~~")); //sink node
        	    else
        		    context.write(key, value);
        	    
        		Text outKey = new Text();
        		Text outValue = new Text();
        		
        		//inital page rank of all nodes
        		double init_rank=(double)1/totalNodes;
        		        		
        		
    			Node node = Node.getNode(value.toString(),init_rank);
    			//check to see if the node is not sink node
    			if(node.getAdjacentNodeNames() != null && node.getAdjacentNodeNames().length > 0) {
    				//compute contribution to send along adjacency list
    			  double outboundPageRank = node.getPageRank() /(double)node.getAdjacentNodeNames().length;
    			  // go through all the nodes and propagate PageRank to them 
    				  for (int i = 0; i < node.getAdjacentNodeNames().length; i++) {
    				    String neighbor = node.getAdjacentNodeNames()[i];
    				    outKey.set(neighbor); 
    				    Node adjacentNode = new Node().setPageRank(outboundPageRank);
    				    outValue.set(adjacentNode.toString());  
    				    context.write(outKey, outValue);
    				  }
    			} 
    			else{     // sum the mass of sink nodes into global counter
    				long val=0;
    				if(Double.parseDouble(value.toString()) == 0.0){ // land here in  first iteration
    					val = (long)(init_rank*100000);
    				}
    				else{   // retreiving the page rank of sink node and adding it in global counter
    				val = (long)(Double.parseDouble(value.toString())*100000);
    				}
    				context.getCounter(Counter.SINK_NODE_MASS_COUNT).increment(val);
    			}
        }
    
}

