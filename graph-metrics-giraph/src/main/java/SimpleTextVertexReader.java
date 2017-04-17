import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.DefaultVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class SimpleTextVertexReader extends VertexReader<IntWritable, Text, Text>{
	
	private RecordReader<LongWritable, Text> lineRecordReader;
	private TaskAttemptContext context;
	
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		this.context = context;
		lineRecordReader = new LineRecordReader();
		lineRecordReader.initialize(inputSplit, context);
	}

	@Override
	public boolean nextVertex() throws IOException, InterruptedException {
		return lineRecordReader.nextKeyValue();
	}

	@Override
	public Vertex<IntWritable, Text, Text> getCurrentVertex() throws IOException, InterruptedException {
		Text line = lineRecordReader.getCurrentValue();
		Vertex<IntWritable, Text, Text> vertex = new DefaultVertex<>();
		String[] words = line.toString().split(" ");
		IntWritable vertexId = new IntWritable(Integer.parseInt(words[0]));
		Text vertexValue = new Text(words[1]);
		List<Edge<IntWritable, Text>> edges = new ArrayList<>();
		for (int i = 2; i < words.length; i += 2) {
			IntWritable destinationId = new IntWritable(Integer.parseInt(words[i]));
			Text label = new Text(words[i + 1]);
			edges.add(EdgeFactory.create(destinationId, label));
		}
		vertex.initialize(vertexId, vertexValue, edges);
		return vertex;
	}

	@Override
	public void close() throws IOException {
		lineRecordReader.close();
		
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lineRecordReader.getProgress();
	}

}
