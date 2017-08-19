package cs6240.neu.pagerank;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

/*
 * Class for representing object from each parsed line from Adjacency list Graph
 * Each line from Adjacency list is parsed and pre processed and stored in this data structure format 
 * */
public class ParsedObject {

	private double initialPageRank=0.0;
	private String Key;
	private String  outlinks;
	public String getKey() {
		return Key;
	}
	public void setKey(String key) {
		Key = key;
	}
	public String getOutlinks() {
		return outlinks;
	}
	public void setOutlinks(List outlinks) { // Modifies the adjacency list to add page rank column as first value in the list
		Set<String> unique_outlinks = new HashSet<String>(outlinks);
		StringBuilder sb = new StringBuilder();
		sb.append(initialPageRank);
		
		if(!unique_outlinks.isEmpty()){
			sb.append("\t")
			  .append(StringUtils.join(unique_outlinks ,"\t"));
		}
		
		this.outlinks= sb.toString();
		
	}
}
