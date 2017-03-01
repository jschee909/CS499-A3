import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CS499MapReduceUser {
    // Class to implement the mapper interface
    static class userMapper extends Mapper<LongWritable, Text, Text, Text> {
        // Map interface of the MapReduce job
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Get the current line
            String line = value.toString();
            // Line is of the format <MovieID,UserID,Rating>
            String[] line_values = line.split("\t");


            context.write(new Text(line_values[1]), new Text(line_values[0] + "\t" + line_values[1] + "\t" + line_values[2]));
        }
    }



public class combiner extends Reducer<Text, Text, Text, Text> {
  @Override
  protected void reduce(final Text key, final Iterable<Text> values,
    final Context context) throws IOException, InterruptedException {
   Integer count = 0;
   final Iterator<Text> itr = values.iterator();
   while (itr.hasNext()) {
    final String text = itr.next().toString();
    String[] line_values = line.split("\t");
    count++;
   }



   context.write(key, new Text(count + "_" + key));
  }
 }


 public class userReducer extends Reducer<Text, Text, Text, Text> {
 @Override
  protected void reduce(final Text key, final Iterable<Text> values,
    final Context context) throws IOException, InterruptedException {
        final Iterator<Text> itr = values.iterator();

        PriorityQueue<String> heap = new PriorityQueue<String>(10, new Comparator(){
            public int compare(String x, String y){
                String[] x_values = x.split("_");
                String[] y_values = y.split("_");
                final Double xNum = Double.parseDouble(x_values[0]);
                final Double yNum = Double.parseDouble(y_values[0]);

                return xNum - yNum;
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
            String[] line_values = text.split("_");
            context.write(new Text(line_values[1]), new Text(line_values[1] + "_" + line_values[0]);
       }



   
   }
 }



    // Main method
    public static void main(String[] args) throws Exception {
        // Check if the arguments are right
        if (args.length != 2) {
            System.err.println("Usage - CS499MapReduceUser <input-file> <output-path>");
            System.exit(-1);
        }

        // Create a job for the mapreduce task
        Job job = new Job();
        job.setJarByClass(CS499MapReduceUser.class);

        // Set the input and output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // set the mapper and reducer class
        job.setMapperClass(userMapper.class);
        job.setReducerClass(userReducer.class);
        jot.setCombinerClass(combiner.class)

        // Set the key and value class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Wait for the job to finish
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}