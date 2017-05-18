package inputformats;

import java.io.IOException;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class EdgeInputFormat extends TextEdgeInputFormat<LongWritable, Text>{
	
	public EdgeInputFormat() {
		super();
	}
	
	protected class mTextEdgeReaderFromEachLine extends TextEdgeReaderFromEachLine {

		@Override
		protected LongWritable getSourceVertexId(Text line) throws IOException {
			return new LongWritable(Long.parseLong(line.toString().split(",")[0]));
		}

		@Override
		protected LongWritable getTargetVertexId(Text line) throws IOException {
			return new LongWritable(Long.parseLong(line.toString().split(",")[1]));
		}

		@Override
		protected Text getValue(Text line) throws IOException {
			return new Text(line.toString().split(",")[2]);
		}
		
	}
	
	@Override
	public EdgeReader<LongWritable, Text> createEdgeReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		return new mTextEdgeReaderFromEachLine();
	}

}
