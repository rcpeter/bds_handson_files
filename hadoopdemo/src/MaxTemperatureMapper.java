import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends  Mapper<LongWritable, Text, Text, Text> {
    
    private static final int TITLE_INDEX = 2;
    private static final int CAST_INDEX = 4;
    
    @Override
    public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
        String line = value.toString();
        
        if (line.startsWith("show_id")) {
            return;
        }
        
        String[] fields = line.split(",");
        String[] castMembers = fields[CAST_INDEX].split(", ");
        
        for (String castMember : castMembers) {
            context.write(new Text(castMember), new Text(fields[TITLE_INDEX]));
        }
    }
}
