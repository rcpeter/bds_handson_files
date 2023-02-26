import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, Text> {

    @Override
    public void reduce(Text castName, Iterable<IntWritable> movieCounts, Context context)
            throws IOException, InterruptedException {

        // Calculate the sum of the movie counts for the cast member
        int sum = 0;
        for (IntWritable count : movieCounts) {
            sum += count.get();
        }

        // If the cast member has appeared in 10 or more movies
        if (sum >= 10) {

            // Emit a key-value pair where the key is the cast member name
            // and the value is the count of movies
            context.write(castName, new Text(Integer.toString(sum)));
        }
    }
}
