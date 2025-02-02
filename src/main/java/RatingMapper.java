// Import necessary classes for MapReduce
import org.apache.hadoop.io.IntWritable; // Hadoop data type that represents an integer value - specifically designed for use in MapReduce programs.
import org.apache.hadoop.io.LongWritable; // Hadoop datta type that represents a long integer vale - used for the key in the map method.
import org.apache.hadoop.io.Text; // Hadoop data type for representing text - used for the value in the map method.
import org.apache.hadoop.mapreduce.Mapper; // Base class for mappers in Hadoop MapReduce.

import java.io.IOException;

// Define the RatingMapper class that extends the Mapper class.
public class RatingMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    // Declare and initialize constants for rating and one.
    private final static IntWritable one = new IntWritable(1); // Create new IntWritable object and assign it a value of 1.
    private final IntWritable rating = new IntWritable(); // Create new IntWritable object and don't give a value for it.

    // Override the map method of the parent class Mapper.
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // Split the line into tokens by the "\t" special character. Separating data with "\t" is common practice for storing structured data, especially in the context of MapReduce.
        String[] tokens = value.toString().split("\t");

        // Extract the rating (third column)
        if (tokens.length == 4) {
            rating.set(Integer.parseInt(tokens[2])); // Get the rating from tokens (a String), make an Integer and set it to the rating IntWritable object.
            context.write(rating, one); // Emit a key-value pair to the Reducer.
        }
    }
}
