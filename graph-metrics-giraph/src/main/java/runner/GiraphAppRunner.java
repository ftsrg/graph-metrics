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

import inputformats.LongTextTextAdjacencyListVertexInputFormat;
import metrics.InDegree;
import metrics.OutDegree;
import metrics.clustering.LocalClusteringCoefficient;
import metrics.dimensionaldegree.DimensionalDegree;
import util.DimensionType;

public class GiraphAppRunner implements Tool {

	private final String RESOURCES_DIR = System.getProperty("user.dir") + "/src/main/resources/";
	private final String INPUT_FILE = "graph_with_triangles.txt";
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
		giraphConf.setVertexInputFormatClass(LongTextTextAdjacencyListVertexInputFormat.class);
		GiraphFileInputFormat.addVertexInputPath(giraphConf, new Path(getInputPath()));

		giraphConf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);

		giraphConf.setWorkerConfiguration(0, 1, 100);
		giraphConf.setLocalTestMode(true);
		giraphConf.setMaxNumberOfSupersteps(10);

		giraphConf.SPLIT_MASTER_WORKER.set(giraphConf, false);
		giraphConf.USE_OUT_OF_CORE_GRAPH.set(giraphConf, true);

//		countInDegree();
//		countOutDegree();
//		countLocalClusteringCoefficient();
		countDimensionalDegree();

		return 1;

	}

	public void countInDegree() throws IOException {
		final String FILE_NAME = "in_degree.txt";
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
		final String FILE_NAME = "out_degree.txt";
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

	public void countLocalClusteringCoefficient() throws IOException {
		final String FILE_NAME = "local_clustering_coefficient.txt";
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
		final String FILE_NAME = "dimensional_degree.txt";
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
