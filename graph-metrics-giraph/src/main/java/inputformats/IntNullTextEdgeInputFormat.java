package inputformats;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for unweighted
 * graphs with int ids.
 *
 * Each line consists of: source_vertex, target_vertex
 */
public class IntNullTextEdgeInputFormat extends TextEdgeInputFormat<LongWritable, Text> {
	/** Splitter for endpoints */
	private static final Pattern SEPARATOR = Pattern.compile("[,]");

	@Override
	public EdgeReader<LongWritable, Text> createEdgeReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		return new IntNullTextEdgeReader();
	}

	/**
	 * {@link org.apache.giraph.io.EdgeReader} associated with
	 * {@link IntNullTextEdgeInputFormat}.
	 */
	public class IntNullTextEdgeReader extends TextEdgeReaderFromEachLineProcessed<String[]> {
		@Override
		protected String[] preprocessLine(Text line) throws IOException {
			String[] tokens = SEPARATOR.split(line.toString());
			return tokens;
		}

		@Override
		protected LongWritable getSourceVertexId(String[] tokens) throws IOException {
			return new LongWritable(Long.valueOf(tokens[0]));
		}

		@Override
		protected LongWritable getTargetVertexId(String[] tokens) throws IOException {
			return new LongWritable(Long.valueOf(tokens[2]));
		}

		@Override
		protected Text getValue(String[] tokens) throws IOException {
			return new Text(tokens[1]);
		}
	}
}
