import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends Reducer<Text, Text, Text, Text> {

	// This method is called once for each key
	public void reduce(Text castName, Iterable<Text> movieNames, Context context)
			throws IOException, InterruptedException {

		// Initialize a counter to count the number of movies
		int movieCount = 0;

		// Initialize a StringBuilder to concatenate the movie names
		StringBuilder movieNameList = new StringBuilder();

		// For each movie name associated with the cast member
		for (Text movieName : movieNames) {

			// Increment the movie count
			movieCount++;

			// Append the movie name to the list
			movieNameList.append(movieName.toString()).append(", ");
		}

		// If the cast member has appeared in 10 or more movies
		if (movieCount >= 10) {

			// Emit a key-value pair where the key is the cast member name
			// and the value is the count of movies and the list of movie names
			context.write(castName, new Text(movieCount + " " + movieNameList));
		}
	}
}
