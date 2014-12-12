package edu.uchicago.mpcs53013.PostSummary;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

import edu.uchicago.mpcs53013.PostSummary.PostSummary;

public class SerializePostSummary {
	static TProtocol protocol;
	static String hadoopPrefix = System.getenv("HADOOP_PREFIX");
	public static void main(String[] args) {
		try {
			System.out.println("Hadoop prefix: " + hadoopPrefix);
			Configuration conf = new Configuration();
			conf.addResource(new Path(hadoopPrefix + "/conf/core-site.xml"));
			conf.addResource(new Path(hadoopPrefix + "/conf/hdfs-site.xml"));
			final Configuration finalConf = new Configuration(conf);
			final FileSystem fs = FileSystem.get(conf);
			final TSerializer ser = new TSerializer(new TBinaryProtocol.Factory());
			final Writer writer = SequenceFile.createWriter(fs, 
	                  finalConf,
	                  new Path("/inputs/thriftPosts/posts"),
                      IntWritable.class, BytesWritable.class);
			PostSummaryProcessor processor = new PostSummaryProcessor() {
//				Map<Integer, SequenceFile.Writer> yearMap = new HashMap<Integer, SequenceFile.Writer>();
//				Pattern yearPattern = Pattern.compile("^\\d+-\\d+-(\\d+)");
				
				Writer getWriter(File file) throws IOException {
//					Matcher yearMatcher = yearPattern.matcher(file.getName());
//					if(!yearMatcher.find())
//						throw new IllegalArgumentException("Bad file name. Can't find year: " + file.getName());
//					int year = Integer.parseInt(yearMatcher.group(1));
//					if(!yearMap.containsKey(year)) {
//						yearMap.put(year, 
//								    SequenceFile.createWriter(fs, 
//								    		                  finalConf,
//								    		                  new Path("/inputs/thriftWeather/weather-" + Integer.toString(year)),
//								                              IntWritable.class, BytesWritable.class));
//					}
//					return yearMap.get(year);
					
					return (SequenceFile.createWriter(fs, 
  		                  finalConf,
  		                  new Path("/inputs/thriftPosts/posts"),
                            IntWritable.class, BytesWritable.class));
				}

				@Override
				void processPostSummary(PostSummary summary, File file) throws IOException {
					try {
						writer.append(new IntWritable(1), new BytesWritable(ser.serialize(summary)));;
					} catch (TException e) {
						throw new IOException(e);
					}
				}
			};
			processor.processPostsDirectory(args[0]);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
