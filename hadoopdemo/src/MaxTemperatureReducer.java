import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends Reducer<Text, Text, Text, Text> {

    private static final int MOVIE_COUNT_THRESHOLD = 10;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> movieCountMap = new HashMap<>();
        for (Text value : values) {
            String movieTitle = value.toString();
            movieCountMap.put(movieTitle, movieCountMap.getOrDefault(movieTitle, 0) + 1);
        }

        int movieCount = 0;
        StringBuilder movieListBuilder = new StringBuilder();
        for (Map.Entry<String, Integer> entry : movieCountMap.entrySet()) {
            int count = entry.getValue();
            movieCount += count;

            if (count > 1) {
                if (movieListBuilder.length() > 0) {
                    movieListBuilder.append(", ");
                }
                movieListBuilder.append(entry.getKey());
            }
        }

        if (movieCount > MOVIE_COUNT_THRESHOLD) {
            String outputValue = String.format("%d %s", movieCount, movieListBuilder.toString());
            context.write(key, new Text(outputValue));
        }
    }
}
