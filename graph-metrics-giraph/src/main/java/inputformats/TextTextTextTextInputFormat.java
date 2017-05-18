package inputformats;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

public class TextTextTextTextInputFormat extends TextVertexInputFormat<LongWritable, Text, Text> {
	/** Separator of the vertex and neighbors */
	private static final Pattern SEPARATOR = Pattern.compile("[,]");

	@Override
	public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
		return new TextTextNullIntVertexReader();
	}

	/**
	 * Vertex reader associated with {@link TextTextNullTextInputFormat}.
	 */
	public class TextTextNullIntVertexReader extends TextVertexReaderFromEachLineProcessed<String[]> {
		/**
		 * Cached vertex id for the current line
		 */

		@Override
		protected String[] preprocessLine(Text line) throws IOException {
			String[] tokens = SEPARATOR.split(line.toString());
			return tokens;
		}

		@Override
		protected LongWritable getId(String[] tokens) throws IOException {
			return new LongWritable(Long.valueOf(tokens[0]));
		}

		@Override
		protected Text getValue(String[] tokens) throws IOException {
			return new Text();
		}

		@Override
		protected Iterable<Edge<LongWritable, Text>> getEdges(String[] tokens) throws IOException {
			List<Edge<LongWritable, Text>> edges = Lists.newArrayListWithCapacity(1);
			edges.add(EdgeFactory.create(new LongWritable(Long.valueOf(tokens[2])), new Text(tokens[1])));
			return edges;
		}
	}
}
