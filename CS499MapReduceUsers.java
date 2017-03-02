import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.*;

import java.io.IOException;

public class CS499MapReduceUsers {
    // Class to implement the mapper interface
    static class userMapper extends Mapper<LongWritable, Text, Text, Text> {
        // Map interface of the MapReduce job
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Get the current line
            String line = value.toString();
            // Line is of the format <userID,UserID,Rating>
            String[] line_values = line.split("\t");


            context.write(new Text(line_values[0]), new Text(line_values[0] + "\t" + line_values[1] + "\t" + line_values[2]));
        }
    }



public class combiner extends Reducer<Text, Text, Text, Text> {
  @Override
  protected void reduce(final Text key, final Iterable<Text> values,
    final Context context) throws IOException, InterruptedException {
   Integer count = 0;
   Double sum = 0D;
   final Iterator<Text> itr = values.iterator();
   while (itr.hasNext()) {
    final String text = itr.next().toString();
    String[] line_values = text.split("\t");
    final Double value = Double.parseDouble(line_values[2]);
    count++;
    sum += value;
   }

   final Double average = sum / count;

   context.write(key, new Text(average + "_" + count + "_" + key));
  }
 }

 public class userReducer extends Reducer<Text, Text, Text, Text> {
  @Override
  protected void reduce(final Text key, final Iterable<Text> values,
    final Context context) throws IOException, InterruptedException {
    final Iterator<Text> itr = values.iterator();

    PriorityQueue<String> heap = new PriorityQueue<String>(10, new Comparator<String>(){
      public int compare(String x, String y){
        String[] x_values = x.split("_");
        String[] y_values = y.split("_");
        final Double xNum = Double.parseDouble(x_values[0]);
        final Double yNum = Double.parseDouble(y_values[0]);

        return (int) (xNum - yNum);
      }
    });
    
     while (itr.hasNext()) {
    final String text = itr.next().toString();
      heap.add(text);
     }

     while(heap.size() > 10){
        heap.poll();
     }

        while(!heap.isEmpty()){
        final String line = heap.poll();
        String[] line_values = line.split("_");
        context.write(new Text(line_values[2]), new Text (line_values[2] + "_" + line_values[0]));
     }



   
   }
}

    // Main method
    public static void main(String[] args) throws Exception {
        // Check if the arguments are right
        if (args.length != 2) {
            System.err.println("Usage - CS499MapReduce <input-file> <output-path>");
            System.exit(-1);
        }

        // Create a job for the mapreduce task
        Job job = new Job();
        job.setJarByClass(CS499MapReduce.class);

        // Set the input and output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // set the mapper and reducer class
        job.setMapperClass(userMapper.class);
        job.setReducerClass(userReducer.class);
        job.setCombinerClass(combiner.class);

        // Set the key and value class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Wait for the job to finish
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}