package packagedemo;

import java.io.*;
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

public class timecount {
        public static void main(String[] args) throws Exception {
                Configuration c = new Configuration();
                String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
                Path input = new Path(files[0]);
                Path output = new Path(files[1]);
                Job j = new Job(c, "WordCount");
                j.setJarByClass(timecount.class);
                j.setMapperClass(MapForWordCount.class);
                j.setReducerClass(ReduceForWordCount.class);
                j.setOutputKeyClass(Text.class);
                j.setOutputValueClass(IntWritable.class);
                FileInputFormat.addInputPath(j, input);
                FileOutputFormat.setOutputPath(j, output);
                System.exit(j.waitForCompletion(true) ? 0 : 1);
        }

        public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

                public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
                        String alltext = value.toString();
                        String[] words = alltext.split("\n");

                        for (String word : words) {
                                String[] lineele = word.split(",");
                                String ip = lineele[1];
                                String[] indatetime = lineele[5].split(" ");
                                String[] outdatetime = lineele[7].split(" ");
                                String intime = indatetime[1];
                                String outtime = outdatetime[1];
                                String[] intimearr = intime.split(":");
                                String[] outtimearr = outtime.split(":");
                                int inhr = Integer.parseInt(intimearr[0]) * 3600;
                                int inmin = Integer.parseInt(intimearr[1]) * 60;
                                int insec = Integer.parseInt(intimearr[2]);
                                int outhr = Integer.parseInt(outtimearr[0]) * 3600;
                                int outmin = Integer.parseInt(intimearr[1]) * 60;
                                int outsec = Integer.parseInt(intimearr[2]);
                                int totalin = inhr + inmin + insec;
                                int totalout = outhr + outmin + outsec;
                                int totallogin = totalout - totalin;

                                con.write(new Text(ip), new IntWritable(totallogin));
                        }
                }
        }

        public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
                int maxt = 0;
                Text maxip = new Text();

                public void reduce(Text key, Iterable<IntWritable> values, Context con)
                                throws IOException, InterruptedException {
                        int curr_time = 0;

                        for (IntWritable value : values) {
                                curr_time += value.get();
                        }

                        if (curr_time > maxt) {
                                maxt = curr_time;
                                maxip = key;
                        }

                }

                protected void cleanup(Context con) throws IOException, InterruptedException {
                        con.write(maxip, new IntWritable(maxt));
                }

        }
}
