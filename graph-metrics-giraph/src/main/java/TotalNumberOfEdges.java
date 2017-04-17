import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;

public class TotalNumberOfEdges extends
  BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

  @Override
  public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
                      Iterable<DoubleWritable> messages) {

    aggregate(TotalNumberOfEdgesMC.ID, new LongWritable(vertex.getNumEdges())); // all we are
      // doing here is sending number of outgoing edges associated with a vertex
      // for which this compute() method was called to an aggregator known
      // under the ID TotalNumberOfEdgesMC.ID

    vertex.voteToHalt();
  }
}
