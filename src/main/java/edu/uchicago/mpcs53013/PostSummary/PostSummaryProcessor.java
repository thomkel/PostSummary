package edu.uchicago.mpcs53013.PostSummary;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import edu.uchicago.mpcs53013.PostSummary.PostSummary;


public abstract class PostSummaryProcessor {
	static class MissingDataException extends Exception {

	    public MissingDataException(String message) {
	        super(message);
	    }

	    public MissingDataException(String message, Throwable throwable) {
	        super(message, throwable);
	    }

	}
	
//	static double tryToReadMeasurement(String name, String s, String missing) throws MissingDataException {
//		if(s.equals(missing))
//			throw new MissingDataException(name + ": " + s);
//		return Double.parseDouble(s.trim());
//	}

	void processLine(String line, File file) throws IOException {
		try {
			processPostSummary(postFromLine(line), file);
		} catch(MissingDataException e) {
			System.out.println("Missing data exception");
		}
	}

	abstract void processPostSummary(PostSummary summary, File file) throws IOException;
	BufferedReader getFileReader(File file) throws FileNotFoundException, IOException {
		if(file.getName().endsWith(".gz"))
			return new BufferedReader
					     (new InputStreamReader
					    		 (new GZIPInputStream
					    				 (new FileInputStream(file))));
		return new BufferedReader(new InputStreamReader(new FileInputStream(file)));
	}
	
	void processPostFile(File post_file) throws IOException {	
		BufferedReader br = getFileReader(post_file);
		br.readLine(); // Discard header
		String line;
    	line=br.readLine();
    	
		int lineNum = 0;
		while(line != null) {
			lineNum++;
			processLine(line, post_file);
			System.out.println("Processed line " + lineNum);
        	line=br.readLine();
		}
	}

	void processPostsDirectory(String directoryName) throws IOException {
		File directory = new File(directoryName);
		File[] directoryListing = directory.listFiles();
		for(File postFile : directoryListing)
			processPostFile(postFile);
	}
	
	PostSummary postFromLine(String line) throws NumberFormatException, MissingDataException {
		String[] lineData = line.split(",");
		String username;
		
		if (lineData.length == 13){
			username = lineData[12];
		}
		else{
			username = "";
		}
		
		PostSummary summary 
			= new PostSummary(lineData[5],
									Integer.parseInt(lineData[0]),
									lineData[3],
				                      lineData[7],
				                      Integer.parseInt(lineData[6]),
				                      username,
				                      Integer.parseInt(lineData[1]),
				                      lineData[2],
				                      Integer.parseInt(lineData[9]),
				                      Integer.parseInt(lineData[10]),
				                      Integer.parseInt(lineData[4]),
				                      Integer.parseInt(lineData[6]),
				                      Integer.parseInt(lineData[8])
				                     );


		return summary;
	}

}
