package inputformats;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Class to read graphs stored as adjacency lists with ids represented by
 * LongWritables and values as Strings.
 */
public class LongTextTextAdjacencyListVertexInputFormat
		extends AdjacencyListTextVertexInputFormat<LongWritable, Text, Text> {

	@Override
	public AdjacencyListTextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) {
		return new LongTextTextAdjacencyListVertexReader(null);
	}

	/**
	 * Vertex reader used with
	 * {@link LongTextTextAdjacencyListVertexInputFormat}
	 */
	protected class LongTextTextAdjacencyListVertexReader extends AdjacencyListTextVertexReader {

		public LongTextTextAdjacencyListVertexReader(LineSanitizer lineSanitizer) {
			super(lineSanitizer);
		}

		@Override
		public LongWritable decodeId(String s) {
			return new LongWritable(Long.valueOf(s));
		}

		@Override
		public Text decodeValue(String s) {
			return new Text(s);
		}

		@Override
		public Edge<LongWritable, Text> decodeEdge(String id, String value) {
			return EdgeFactory.create(new LongWritable(Long.valueOf(id)), new Text(value));
		}

	}

}
