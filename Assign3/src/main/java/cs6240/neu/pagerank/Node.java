
package cs6240.neu.pagerank;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
/*
 * This data structure is used for holding  the adjacency list representaion of each node 
 * It is used for setting and getting pagerank and adjacency list of each node.
 * Source : StackOverflow
 * */

public class Node {
	private double pageRank = 0.0;
	private String[] adjacentNodeNames;
	private boolean sinkNodeflag=false;

	public static final char fieldSeparator = '\t';

	public double getPageRank() {
		return pageRank;
	}

	public Node setPageRank(double pageRank) {
		this.pageRank = pageRank;
		return this;
	}

	public String[] getAdjacentNodeNames() {
		return adjacentNodeNames;
	}

	public Node setAdjacentNodeNames(String[] adjacentNodeNames) {
		this.adjacentNodeNames = adjacentNodeNames;
		return this;
	}

	public boolean containsAdjacentNodes() {
		return adjacentNodeNames != null;
	}

	public Node setSinkNode(boolean flag) {
		this.sinkNodeflag = flag;
		return this;
	}

	public boolean isSinkNode(){
		return sinkNodeflag;
	}
	@Override
	public String toString() {     // String representation of adjacency list
		StringBuilder sb = new StringBuilder();
		sb.append(pageRank);

		if (getAdjacentNodeNames() != null) {
			sb.append(fieldSeparator)
			.append(StringUtils
					.join(getAdjacentNodeNames(), fieldSeparator));
		}
		return sb.toString();
	}

	public static Node getNode(String value,double init_pr) throws IOException {
		String[] parts = StringUtils.splitPreserveAllTokens(
				value, fieldSeparator);

		if(parts.length<1){
			throw new IOException("INVALID GRAPH");
		}

		Node node = new Node();
		if(Double.valueOf(parts[0])== 0.0){  //setting the initial page rank
			node.setPageRank(init_pr);
		}
		else{
			node.setPageRank(Double.valueOf(parts[0]));
		}
		if (parts.length > 1) {
			
			if (parts[1].equalsIgnoreCase("~~")) { //check if it is a sink node
				node.setSinkNode(true);
				return node;
			}

			node.setAdjacentNodeNames(Arrays.copyOfRange(parts, 1,parts.length));  
		}
		return node;
	}
}