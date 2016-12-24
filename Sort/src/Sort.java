


import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Sort {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final TreeMap<Integer, Text> sortMap = new TreeMap <Integer, Text>();
    
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
       
    	String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        
        String[] mots;

        
        while (tokenizer.hasMoreTokens()) {
           
        	 mots = tokenizer.nextToken().split(",");
        	 sortMap.put(new Integer(mots[1]),  new Text(mots[0]));
        }
        
        
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	for (Integer k : sortMap.keySet()) {
    		context.write(new Text(sortMap.get(k)),new IntWritable(k));
        } 
    }
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	private static final TreeMap<Integer, Text> sortMap = new TreeMap <Integer, Text>();
	 
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
    
        for (IntWritable val : values) {
            sortMap.put(val.get(),new Text(key));
            
        }
    } 

	 @Override
	 protected void cleanup(Context context) throws IOException, InterruptedException {
	        for (Integer k : sortMap.keySet()) {;
	             context.write(new Text(sortMap.get(k)),new IntWritable(k));
	        } 
	 }
 }
        
 public static void main(String[] args) throws Exception {
	 
    Configuration conf = new Configuration();
   
    Job job = new Job(conf, "Sort");
  
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    System.out.println("Sort ");    
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setNumReduceTasks(1);
    
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.waitForCompletion(true);   
}
        
}
