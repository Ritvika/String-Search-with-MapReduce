import java.io.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class String_Search_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private IntWritable zero = new IntWritable();
    
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String str = value.toString();
        str = str.toLowerCase();  
        String[] lines = str.split(System.getProperty("line.separator"));
        int size = lines.length;
        for(int i = 0; i < size; i++)
        {
        	str = lines[i];
        	str = str.toLowerCase();		        	
	        Pattern p1 = Pattern.compile("chicago");                                   //Match pattern for Chicago
	        Matcher m1 = p1.matcher(str);
	        word.set("Chicago");
	        if (m1.find()) { 	        	
                context.write(word, one);	                                    
	        }
	        else {
	        	context.write(word, zero);
	        }
	        Pattern p2 = Pattern.compile("dec");                                       //Match pattern for Dec
	        Matcher m2 = p2.matcher(str);
	        word.set("Dec");
	        if (m2.find()) { 	        	
                context.write(word, one);
            }
	        else {
	        	context.write(word, zero);
	        }
	        Pattern p3 = Pattern.compile("java");                                      //Match pattern for Java
	        Matcher m3 = p3.matcher(str);
	        word.set("Java");
	        if (m3.find()) { 	        	
                context.write(word, one);
            }
	        else {
	        	context.write(word, zero);
	        }
	        Pattern p4 = Pattern.compile("hackathon");                                 //Match pattern for hackathon
	        Matcher m4 = p4.matcher(str);
	        word.set("hackathon");
	        if (m4.find()) { 	        	
                context.write(word, one);
	        }
	        else {
	        	context.write(word, zero);
	        }
        }
    }
}



