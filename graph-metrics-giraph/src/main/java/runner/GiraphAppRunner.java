package runner;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import inputformats.TextTextTextTextInputFormat;
import metrics.clustering.LocalClusteringCoefficient;
import metrics.degrees.AverageDegree;
import metrics.degrees.InDegree;
import metrics.degrees.OutDegree;
import metrics.dimensionalclustering.DimensionalClustering1;
import metrics.dimensionalclustering.DimensionalClustering2;
import metrics.dimensionaldegree.DimensionalDegree;
import metrics.edgedimensionactivity.EdgeDimensionActivity;
import metrics.edgedimensionconnectivity.EdgeDimensionConnectivity;
import metrics.mpc.MultiplexParticipationCoefficient;
import metrics.nedc.NodeExclusiveDimensionConnectivity;
import metrics.nodeactivity.NodeActivity;
import metrics.nodedimensionactivity.NodeDimensionActivity;
import metrics.nodedimensionconnectivity.NodeDimensionConnectivity;
import test.Test;

public class GiraphAppRunner implements Tool {

	private final String RESOURCES_DIR = System.getProperty("user.dir") + "/src/main/resources/";
	private final String INPUT_FILE = "big_graph";
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
//		giraphConf.setVertexInputFormatClass(LongTextTextAdjacencyListVertexInputFormat.class);
		giraphConf.setVertexInputFormatClass(TextTextTextTextInputFormat.class);
		GiraphFileInputFormat.addVertexInputPath(giraphConf, new Path(getInputPath()));

		giraphConf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
		giraphConf.setWorkerConfiguration(0, 1, 100);
		giraphConf.setLocalTestMode(true);
		giraphConf.setMaxNumberOfSupersteps(10);

		giraphConf.SPLIT_MASTER_WORKER.set(giraphConf, false);
		giraphConf.USE_OUT_OF_CORE_GRAPH.set(giraphConf, true);
		
//		test();
		countInDegree();
//		countOutDegree();
		countAverageDegree();
//		countLocalClusteringCoefficient();
//		countDimensionalDegree();
//		countNodeDimensionActivity();
//		countNodeDimensionConnectivity();
//		countNodeExclusiveDimensionConnectivity();
//		countEdgeDimensionActivity();
//		countEdgeDimensionConnectivity();
//		countNodeActivity();
//		countMultiplexParticipationCoefficient();
//		countDimensionalClustering1();
//		countDimensionalClustering2();
		
		return 1;

	}

	public void test() throws IOException {
		final String FILE_NAME = "test";
		FileUtils.deleteDirectory(new File(RESOURCES_DIR + FILE_NAME));
		setOutputPath(RESOURCES_DIR + FILE_NAME);
		giraphConf.setComputationClass(Test.class);
		GiraphJob testJob;
		try {
			testJob = new GiraphJob(giraphConf, getClass().getName());
			FileOutputFormat.setOutputPath(testJob.getInternalJob(), new Path(getOutputPath()));
			testJob.run(true);
		} catch (IOException | ClassNotFoundException | InterruptedException exception) {
			LOG.error(exception);
		}
	}
	
	public void countInDegree() throws IOException {
		final String FILE_NAME = "in_degree";
		FileUtils.deleteDirectory(new File(RESOURCES_DIR + FILE_NAME));
		setOutputPath(RESOURCES_DIR + FILE_NAME);
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

	public void countOutDegree() throws IOException {
		final String FILE_NAME = "out_degree";
		FileUtils.deleteDirectory(new File(RESOURCES_DIR + FILE_NAME));
		setOutputPath(RESOURCES_DIR + FILE_NAME);
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
	
	public void countAverageDegree() throws IOException {
		final String FILE_NAME = "average_degree";
		FileUtils.deleteDirectory(new File(RESOURCES_DIR + FILE_NAME));
		setOutputPath(RESOURCES_DIR + FILE_NAME);
		giraphConf.setComputationClass(AverageDegree.class);
		GiraphJob averageDegreeJob;
		try {
			averageDegreeJob = new GiraphJob(giraphConf, getClass().getName());
			FileOutputFormat.setOutputPath(averageDegreeJob.getInternalJob(), new Path(getOutputPath()));
			averageDegreeJob.run(true);
		} catch (IOException | ClassNotFoundException | InterruptedException exception) {
			LOG.error(exception);
		}
	}

	public void countLocalClusteringCoefficient() throws IOException {
		final String FILE_NAME = "local_clustering_coefficient";
		FileUtils.deleteDirectory(new File(RESOURCES_DIR + FILE_NAME));
		setOutputPath(RESOURCES_DIR + FILE_NAME);
		giraphConf.setComputationClass(LocalClusteringCoefficient.LCCComputation.class);
		giraphConf.setMasterComputeClass(LocalClusteringCoefficient.MasterCompute.class);
		GiraphJob localClusteringCoefficientJob;
		try {
			localClusteringCoefficientJob = new GiraphJob(giraphConf, getClass().getName());
			FileOutputFormat.setOutputPath(localClusteringCoefficientJob.getInternalJob(), new Path(getOutputPath()));
			localClusteringCoefficientJob.run(true);
		} catch (IOException | ClassNotFoundException | InterruptedException exception) {
			LOG.error(exception);
		}
	}

	public void countDimensionalDegree() throws IOException {
		final String FILE_NAME = "dimensional_degree";
		FileUtils.deleteDirectory(new File(RESOURCES_DIR + FILE_NAME));
		setOutputPath(RESOURCES_DIR + FILE_NAME);
		giraphConf.setComputationClass(DimensionalDegree.DimensionalDegreeComputation.class);
		giraphConf.setMasterComputeClass(DimensionalDegree.MasterCompute.class);
		GiraphJob dimensionalDegreeJob;
		try {
			dimensionalDegreeJob = new GiraphJob(giraphConf, getClass().getName());
			FileOutputFormat.setOutputPath(dimensionalDegreeJob.getInternalJob(), new Path(getOutputPath()));
			dimensionalDegreeJob.run(true);
		} catch (IOException | ClassNotFoundException | InterruptedException exception) {
			LOG.error(exception);
		}
	}

	public void countNodeDimensionActivity() throws IOException {
		final String FILE_NAME = "node_dimension_activity";
		FileUtils.deleteDirectory(new File(RESOURCES_DIR + FILE_NAME));
		setOutputPath(RESOURCES_DIR + FILE_NAME);
		giraphConf.setComputationClass(NodeDimensionActivity.NodeDimensionActivityComputation.class);
		giraphConf.setMasterComputeClass(NodeDimensionActivity.MasterCompute.class);
		GiraphJob nodeDimensionActivityJob;
		try {
			nodeDimensionActivityJob = new GiraphJob(giraphConf, getClass().getName());
			FileOutputFormat.setOutputPath(nodeDimensionActivityJob.getInternalJob(), new Path(getOutputPath()));
			nodeDimensionActivityJob.run(true);
		} catch (IOException | ClassNotFoundException | InterruptedException exception) {
			LOG.error(exception);
		}
	}

	public void countNodeDimensionConnectivity() throws IOException {
		final String FILE_NAME = "node_dimension_connectivity";
		FileUtils.deleteDirectory(new File(RESOURCES_DIR + FILE_NAME));
		setOutputPath(RESOURCES_DIR + FILE_NAME);
		giraphConf.setComputationClass(NodeDimensionConnectivity.NodeDimensionConnectivityComputation.class);
		giraphConf.setMasterComputeClass(NodeDimensionConnectivity.MasterCompute.class);
		GiraphJob nodeDimensionConnectivityJob;
		try {
			nodeDimensionConnectivityJob = new GiraphJob(giraphConf, getClass().getName());
			FileOutputFormat.setOutputPath(nodeDimensionConnectivityJob.getInternalJob(), new Path(getOutputPath()));
			nodeDimensionConnectivityJob.run(true);
		} catch (IOException | ClassNotFoundException | InterruptedException exception) {
			LOG.error(exception);
		}
	}
	
	public void countNodeExclusiveDimensionConnectivity() throws IOException {
		final String FILE_NAME = "node_exclusive_dimension_connectivity";
		FileUtils.deleteDirectory(new File(RESOURCES_DIR + FILE_NAME));
		setOutputPath(RESOURCES_DIR + FILE_NAME);
		giraphConf.setComputationClass(NodeExclusiveDimensionConnectivity.AggregateResult.class);
		giraphConf.setMasterComputeClass(NodeExclusiveDimensionConnectivity.MasterCompute.class);
		GiraphJob NEDCJob;
		try {
			NEDCJob = new GiraphJob(giraphConf, getClass().getName());
			FileOutputFormat.setOutputPath(NEDCJob.getInternalJob(), new Path(getOutputPath()));
			NEDCJob.run(true);
		} catch (IOException | ClassNotFoundException | InterruptedException exception) {
			LOG.error(exception);
		}
	}
	
	public void countEdgeDimensionActivity() throws IOException {
		final String FILE_NAME = "edge_dimension_activity";
		FileUtils.deleteDirectory(new File(RESOURCES_DIR + FILE_NAME));
		setOutputPath(RESOURCES_DIR + FILE_NAME);
		giraphConf.setComputationClass(EdgeDimensionActivity.EdgeDimensionActivityComputation.class);
		giraphConf.setMasterComputeClass(EdgeDimensionActivity.MasterCompute.class);
		GiraphJob edgeDimensionActivityJob;
		try {
			edgeDimensionActivityJob = new GiraphJob(giraphConf, getClass().getName());
			FileOutputFormat.setOutputPath(edgeDimensionActivityJob.getInternalJob(), new Path(getOutputPath()));
			edgeDimensionActivityJob.run(true);
		} catch (IOException | ClassNotFoundException | InterruptedException exception) {
			LOG.error(exception);
		}
	}

	public void countEdgeDimensionConnectivity() throws IOException {
		final String FILE_NAME = "edge_dimension_connectivity";
		FileUtils.deleteDirectory(new File(RESOURCES_DIR + FILE_NAME));
		setOutputPath(RESOURCES_DIR + FILE_NAME);
		giraphConf.setComputationClass(EdgeDimensionConnectivity.EdgeDimensionConnectivityComputation.class);
		giraphConf.setMasterComputeClass(EdgeDimensionConnectivity.MasterCompute.class);
		GiraphJob edgeDimensionConnectivityJob;
		try {
			edgeDimensionConnectivityJob = new GiraphJob(giraphConf, getClass().getName());
			FileOutputFormat.setOutputPath(edgeDimensionConnectivityJob.getInternalJob(), new Path(getOutputPath()));
			edgeDimensionConnectivityJob.run(true);
		} catch (IOException | ClassNotFoundException | InterruptedException exception) {
			LOG.error(exception);
		}
	}
	
	public void countNodeActivity() throws IOException {
		final String FILE_NAME = "node_activity";
		FileUtils.deleteDirectory(new File(RESOURCES_DIR + FILE_NAME));
		setOutputPath(RESOURCES_DIR + FILE_NAME);
		giraphConf.setComputationClass(NodeActivity.NodeActivityComputation.class);
		giraphConf.setMasterComputeClass(NodeActivity.MasterCompute.class);
		GiraphJob nodeActivityJob;
		try {
			nodeActivityJob = new GiraphJob(giraphConf, getClass().getName());
			FileOutputFormat.setOutputPath(nodeActivityJob.getInternalJob(), new Path(getOutputPath()));
			nodeActivityJob.run(true);
		} catch (IOException | ClassNotFoundException | InterruptedException exception) {
			LOG.error(exception);
		}
	}
	
	public void countMultiplexParticipationCoefficient() throws IOException {
		final String FILE_NAME = "multiplex_participation_coefficient";
		FileUtils.deleteDirectory(new File(RESOURCES_DIR + FILE_NAME));
		setOutputPath(RESOURCES_DIR + FILE_NAME);
		giraphConf.setComputationClass(MultiplexParticipationCoefficient.MultiplexParticipationCoefficientComputation.class);
		giraphConf.setMasterComputeClass(MultiplexParticipationCoefficient.MasterCompute.class);
		GiraphJob mpcJob;
		try {
			mpcJob = new GiraphJob(giraphConf, getClass().getName());
			FileOutputFormat.setOutputPath(mpcJob.getInternalJob(), new Path(getOutputPath()));
			mpcJob.run(true);
		} catch (IOException | ClassNotFoundException | InterruptedException exception) {
			LOG.error(exception);
		}
	}
	
	public void countDimensionalClustering1() throws IOException {
		final String FILE_NAME = "dimensional_clustering_1";
		FileUtils.deleteDirectory(new File(RESOURCES_DIR + FILE_NAME));
		setOutputPath(RESOURCES_DIR + FILE_NAME);
		giraphConf.setComputationClass(DimensionalClustering1.DimensionalClustering1Computation.class);
		giraphConf.setMasterComputeClass(DimensionalClustering1.MasterCompute.class);
		GiraphJob dc1Job;
		try {
			dc1Job = new GiraphJob(giraphConf, getClass().getName());
			FileOutputFormat.setOutputPath(dc1Job.getInternalJob(), new Path(getOutputPath()));
			dc1Job.run(true);
		} catch (IOException | ClassNotFoundException | InterruptedException exception) {
			LOG.error(exception);
		}
	}
	
	public void countDimensionalClustering2() throws IOException {
		final String FILE_NAME = "dimensional_clustering_2";
		FileUtils.deleteDirectory(new File(RESOURCES_DIR + FILE_NAME));
		setOutputPath(RESOURCES_DIR + FILE_NAME);
		giraphConf.setComputationClass(DimensionalClustering2.DimensionalClustering2Computation.class);
		giraphConf.setMasterComputeClass(DimensionalClustering2.MasterCompute.class);
		GiraphJob dc2Job;
		try {
			dc2Job = new GiraphJob(giraphConf, getClass().getName());
			FileOutputFormat.setOutputPath(dc2Job.getInternalJob(), new Path(getOutputPath()));
			dc2Job.run(true);
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
