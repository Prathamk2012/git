package packagedemo;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class countmusic {

	public static void main(String [] args) throws Exception{
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c,args).getRemainingArgs();
        Path input=new Path(files[0]);
        
        Path output=new Path(files[1]);
        Job j=new Job(c,"WordCount");
        j.setJarByClass(countmusic.class);
        j.setMapperClass(MapForWordCount.class);
        j.setReducerClass(ReduceForWordCount.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        
        Path output1=new Path(files[2]);
        Job j1=new Job(c,"WordCount1");
        j1.setJarByClass(countmusic.class);
        j1.setMapperClass(MapForWordCount1.class);
        j1.setReducerClass(ReduceForWordCount1.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, output1);
        
        System.exit(j.waitForCompletion(true) && j1.waitForCompletion(true)?0:1);
}


public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {


public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
        String alltext = value.toString();
        String[] lines = alltext.split("\n");
        
        for(String line:lines){
                
                String[] row =  line.split(",");
                
                Text trackid = new Text(row[1].toString());
                IntWritable output_value = new IntWritable(Integer.parseInt(row[3].toString()));
                
                
                con.write(trackid, output_value);
        }             
}
}


public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {


public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
        int count = 0;
        
        for(IntWritable num:values){
                count+=num.get();
        }
        
        Text op = new Text("Track listned: "+key.toString());
        
        con.write(op, new IntWritable(count));
}


}

public static class MapForWordCount1 extends Mapper<LongWritable, Text, Text, IntWritable> {


public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
        String alltext = value.toString();
        String[] lines = alltext.split("\n");
        
        for(String line:lines){
                
                String[] row =  line.split(",");
                
                Text trackid = new Text(row[1].toString());
                IntWritable output_value = new IntWritable(Integer.parseInt(row[4].toString()));
                
                
                con.write(trackid, output_value);
        }             
}
}


public static class ReduceForWordCount1 extends Reducer<Text, IntWritable, Text, IntWritable> {


public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
        int count = 0;
        
        for(IntWritable num:values){
                count+=num.get();
        }
        
        Text op = new Text("Track skipped: "+key.toString());
        
        con.write(op, new IntWritable(count));
}


}

}
