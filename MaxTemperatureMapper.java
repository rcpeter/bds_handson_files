import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 * @Author: S. Ray for demo
 */

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable inputKey, Text inputValue, Context context)
            throws IOException, InterruptedException {

        // Convert input record from Text to String
        String inputLine = inputValue.toString();

        // Split the input record using "," as delimiter
        String[] inputFields = inputLine.split(",");

        // Extract the movie name from the third field of input record
        String movieName = inputFields[2].trim();

        // Define a pattern to match a string within quotes
        Pattern quotedStringPattern = Pattern.compile("\"([^\"]+)\"");

        // Create a matcher object to search for quoted strings in input record
        Matcher quotedStringMatcher = quotedStringPattern.matcher(inputLine);

        // If a quoted string is found in input record
        if (quotedStringMatcher.find()) {

            // Get the matched string
            String castString = quotedStringMatcher.group(0);

            // Split the matched string using "," as delimiter
            String[] allCasts = castString.split(",");

            // For each cast member in the matched string
            for (int i = 0; i < allCasts.length; i++) {

                // Remove the quotes from the cast member name
                String castName = allCasts[i].replaceAll("\"", "");

                // Emit a key-value pair where the key is the cast member name
                // and the value is the name of the movie they appeared in
                context.write(new Text(castName), new Text(movieName));
            }
        }
    }
}
