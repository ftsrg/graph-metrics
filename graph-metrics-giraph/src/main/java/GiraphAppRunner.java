import java.io.IOException;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat;
import org.apache.giraph.io.formats.LongDoubleDoubleAdjacencyListVertexInputFormat;
import org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat;
import org.apache.giraph.io.formats.TextDoubleDoubleAdjacencyListVertexInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import clustering.ClusteringCoefficient;
import inputformats.EdgeInputFormat;
import inputformats.LongTextTextAdjacencyListVertexInputFormat;

public class GiraphAppRunner implements Tool{

	private final String RESOURCES_DIR = System.getProperty("user.dir") + "/src/main/resources/";
	private final String INPUT_FILE = "LongTextTextAdjacencyList.txt";
	private Configuration conf;
	private String inputPath;
	private String outputPath;
	private GiraphConfiguration giraphConf;
	private static final Logger LOG = Logger.getLogger(GiraphAppRunner.class);
	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		setInputPath(RESOURCES_DIR + INPUT_FILE);

		giraphConf = new GiraphConfiguration();

//		giraphConf.setVertexInputFormatClass(JsonLongDoubleFloatDoubleVertexInputFormat.class);
//		giraphConf.setVertexInputFormatClass(LongDoubleDoubleAdjacencyListVertexInputFormat.class);
//		giraphConf.setVertexInputFormatClass(TextDoubleDoubleAdjacencyListVertexInputFormat.class);
		giraphConf.setVertexInputFormatClass(LongTextTextAdjacencyListVertexInputFormat.class);
		GiraphFileInputFormat.addVertexInputPath(giraphConf, new Path(getInputPath()));
		
		giraphConf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
		
		giraphConf.setWorkerConfiguration(0, 1, 100);
		giraphConf.setLocalTestMode(true);
		giraphConf.setMaxNumberOfSupersteps(10);

		giraphConf.SPLIT_MASTER_WORKER.set(giraphConf, false);
		giraphConf.USE_OUT_OF_CORE_GRAPH.set(giraphConf, true);

		countInDegree();
//		countOutDegree();
//		countLocalClusteringCoefficient();
//		countTotalNumberOfEdges();
//		simpleMasterComputation();

		return 1;

	}

	public void countInDegree() {
		setOutputPath(RESOURCES_DIR + "in_degree.txt");
		giraphConf.setComputationClass(InDegree.class);
		GiraphJob inDegreeJob;
		try {
			inDegreeJob = new GiraphJob(giraphConf, getClass().getName());
			FileOutputFormat.setOutputPath(inDegreeJob.getInternalJob(), new Path(getOutputPath()));
			inDegreeJob.run(true);
		} catch (IOException | ClassNotFoundException | InterruptedException exception) {
			LOG.error(exception);
		}
	}

	public void countOutDegree() {
		setOutputPath(RESOURCES_DIR + "out_degree.txt");
		giraphConf.setComputationClass(OutDegree.class);
		GiraphJob outDegreeJob;
		try {
			outDegreeJob = new GiraphJob(giraphConf, getClass().getName());
			FileOutputFormat.setOutputPath(outDegreeJob.getInternalJob(), new Path(getOutputPath()));
			outDegreeJob.run(true);
		} catch (IOException | ClassNotFoundException | InterruptedException exception) {
			LOG.error(exception);
		}
	}

	public void countLocalClusteringCoefficient() {
		setOutputPath(RESOURCES_DIR + "local_clustering_coefficient.txt");
		giraphConf.setComputationClass(ClusteringCoefficient.ClusteringCoefficientComputation.class);
		GiraphJob localClusteringCoefficientJob;
		try {
			localClusteringCoefficientJob = new GiraphJob(giraphConf, getClass().getName());
			FileOutputFormat.setOutputPath(localClusteringCoefficientJob.getInternalJob(), new Path(getOutputPath()));
			localClusteringCoefficientJob.run(true);
		} catch (IOException | ClassNotFoundException | InterruptedException exception) {
			LOG.error(exception);
		}
	}

	public void countTotalNumberOfEdges() {
		setOutputPath(RESOURCES_DIR + "total_number_of_edges.txt");
		giraphConf.setComputationClass(TotalNumberOfEdges.class);
		GiraphJob totalNumberOfEdgesJob;
		try {
			totalNumberOfEdgesJob = new GiraphJob(giraphConf, getClass().getName());
			FileOutputFormat.setOutputPath(totalNumberOfEdgesJob.getInternalJob(), new Path(getOutputPath()));
			totalNumberOfEdgesJob.run(true);
		} catch (IOException | ClassNotFoundException | InterruptedException exception) {
			LOG.error(exception);
		}
	}

	public void simpleMasterComputation() {
		setOutputPath(RESOURCES_DIR + "simple_master_computation.txt");
		giraphConf.setComputationClass(SimpleMasterComputeComputation.class);
		GiraphJob simpleMasterComputeJob;
		try {
			simpleMasterComputeJob = new GiraphJob(giraphConf, getClass().getName());
			FileOutputFormat.setOutputPath(simpleMasterComputeJob.getInternalJob(), new Path(getOutputPath()));
			simpleMasterComputeJob.run(true);
		} catch (IOException | ClassNotFoundException | InterruptedException exception) {
			LOG.error(exception);
		}
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new GiraphAppRunner(), args);
	}

	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

}
