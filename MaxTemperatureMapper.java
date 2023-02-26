import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context)
            throws IOException, InterruptedException {

        // Split the input line into fields
        String inputLine = inputValue.toString();
        String[] fields = inputLine.split(",");

        // Extract the movie name from the third field
        String movieName = fields[2].trim();

        // Extract the cast names from the quoted strings in the input line
        Pattern quotedStringPattern = Pattern.compile("\"([^\"]+)\"");
        Matcher quotedStringMatcher = quotedStringPattern.matcher(inputLine);

        while (quotedStringMatcher.find()) {
            String castString = quotedStringMatcher.group(0);
            String[] allCasts = castString.split(",");
            for (String cast : allCasts) {
                String castName = cast.replaceAll("\"", "").trim();
                context.write(new Text(castName), new IntWritable(1));
            }
        }
    }
}
