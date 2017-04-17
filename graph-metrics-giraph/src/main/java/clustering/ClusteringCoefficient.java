package clustering;

import java.io.IOException;
import java.util.HashSet;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class ClusteringCoefficient {
	/**
	   * Used to aggregate the local clustering coefficients, and compute the 
	   * global one.
	   */
//	  private static String CL_COEFFICIENT_AGGREGATOR = "coefficient.aggregator";

	  /**
	   * Aggregator used to store the global clustering coefficient.
	   */
//	  public static String GLOBAL_CLUSTERING_COEFFICIENT = 
//	      "global.clustering.coefficient";

//	  public static String COUNTER_GROUP = "Clustering Coefficient";
//	  public static String COUNTER_NAME = "Global (x1000)";

	  public static class SendFriendsList extends SendFriends<LongWritable, 
	    DoubleWritable, NullWritable, LongIdFriendsList> {
	  }

	  public static class ClusteringCoefficientComputation extends BasicComputation<
	  LongWritable, DoubleWritable, NullWritable, LongIdFriendsList> {

	    @Override
	    public void compute(
	        Vertex<LongWritable, DoubleWritable, NullWritable> vertex,
	        Iterable<LongIdFriendsList> messages)
	            throws IOException {
	    	
	      // Add the friends of this vertex in a HashSet so that we can check 
	      // for the existence of triangles quickly.
	      HashSet<LongWritable> friends = new HashSet<LongWritable>();
	      for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
	        friends.add(new LongWritable(edge.getTargetVertexId().get()));
	      }

	      int edges = vertex.getNumEdges();
	      int triangles = 0;
	      for (LongIdFriendsList msg : messages) {
	        for (LongWritable id : msg.getMessage()) {
	          if (friends.contains(id)) {
	            // Triangle found
	            triangles++;
	          }
	        }
	      }
	      
	      double clusteringCoefficient = 
	          ((double)triangles) / ((double)edges*(edges-1));

	      DoubleWritable clCoefficient = new DoubleWritable(clusteringCoefficient);
	      //aggregate(CL_COEFFICIENT_AGGREGATOR, clCoefficient);
	      aggregate(CLCoefficientAggregator.CL_COEFFICIENT_AGGREGATOR, clCoefficient);
	      vertex.setValue(clCoefficient);
	      vertex.voteToHalt();
	    }
	  }

	  public static class LongIdFriendsList extends MessageWrapper<LongWritable, 
	  LongArrayListWritable> { 

	    @Override
	    public Class<LongWritable> getVertexIdClass() {
	      return LongWritable.class;
	    }

	    @Override
	    public Class<LongArrayListWritable> getMessageClass() {
	      return LongArrayListWritable.class;
	    }
	  }



	  /**
	   * Coordinates the execution of the algorithm.
	   */
//	  public static class MasterCompute extends DefaultMasterCompute {
//
//	    @Override
//	    public final void initialize() throws InstantiationException,
//	        IllegalAccessException {
//
//	      registerAggregator(CL_COEFFICIENT_AGGREGATOR, DoubleSumAggregator.class);
//	    }
//
//	    @Override
//	    public final void compute() {
//	      long superstep = getSuperstep();
//	      if (superstep == 0) {
//	        setComputation(SendFriendsList.class);
//	      } else {
//	        setComputation(ClusteringCoefficientComputation.class);
//	      }
//	      if (superstep == 2) {
//	        double partialSum = ((DoubleWritable)getAggregatedValue(
//	            CL_COEFFICIENT_AGGREGATOR)).get();
//	        double globalCoefficient = partialSum/(double)getTotalNumVertices();
//	        Counters.updateCounter(getContext(), COUNTER_GROUP, COUNTER_NAME,
//	            (long)(1000*globalCoefficient));
//	      }
//	    }
//	  }
}
