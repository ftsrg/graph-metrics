import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.AdjacencyListTextVertexOutputFormat;
import org.apache.giraph.io.formats.TextDoubleDoubleAdjacencyListVertexInputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

public class GiraphHelloWorld extends BasicComputation<IntWritable, IntWritable, NullWritable, NullWritable> {
	final static String[] graphSeed = new String[] { "seed\t0" };
	
	@Override
	public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex, Iterable<NullWritable> messages) {
		System.out.print("Hello world from the: " + vertex.getId().toString() + " who is following:");

		for (Edge<IntWritable, NullWritable> e : vertex.getEdges()) {
			System.out.print(" " + e.getTargetVertexId());
		}
		System.out.println("");

		vertex.voteToHalt(); // signaling the end of the current BSP computation for the current vertex
	}

	public void run() {
		GiraphConfiguration conf = new GiraphConfiguration(); // Giraph configuration
		conf.setComputationClass(GenerateTwitterParallel.class);
		conf.setVertexInputFormatClass(TextDoubleDoubleAdjacencyListVertexInputFormat.class);
		conf.setVertexOutputFormatClass(AdjacencyListTextVertexOutputFormat.class);
		try {
			Iterable<String> results = InternalVertexRunner.run(conf, graphSeed);
			for (String s : results) {
				System.out.println(s);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
