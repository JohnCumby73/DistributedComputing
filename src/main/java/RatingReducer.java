import org.apache.hadoop.io.IntWritable; // Hadoop data type that represents an integer value - specifically designed for use in MapReduce programs.
import org.apache.hadoop.mapreduce.Reducer; // A Hadoop MapReduce package. Provides the foundation for writing Reducer functions in MapReduce programs.

import java.io.IOException;

// Define the RatingReducer class that extends the Reducer class.
public class RatingReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    // Create new IntWritable object to store the calculated sum of ratings.
    private final IntWritable result = new IntWritable();

    // Override the map method of the parent class Mapper.
    @Override

    // This method is called once for each unique key emitted by the Mapper. In this instance, 5 times.
    // The first time it's called, it will iterate over every key-value pair with a key of 1,
    // add `1` to the sum each time, and emit the result to the next stage of the MapReduce job.

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        try {
            int sum = 0; //Initialize sum variable to keep track of the total count for the current rating.
            // Iterate over all values.
            for (IntWritable val : values) {
                // Add each value to the sum for that value.
                sum += val.get();
            }
            result.set(sum); // Set the result variable to the calculated sum.
            context.write(key, result); // Emit the key (the rating) and the result (the total count) to the next stage of the job.
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
