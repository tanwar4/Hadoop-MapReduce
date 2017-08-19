package org.com.mr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
//a loader routine that takes an input filename, reads the file, and returns a String[] or List<String>
//containing the lines of the file
public final class LoaderRoutine {
	
	public static List<String> readfile(String inputfile){
		BufferedReader reader = null;
		//output list where parsed data get stored
		List<String> outputList = new  ArrayList<>();
		
		try{
			reader = new BufferedReader(new FileReader(inputfile));
			String line=null;			
			
			while(true){
				line = reader.readLine();
				if(line == null) break;
				outputList.add(line);
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}
	
		return outputList;
	}
}
