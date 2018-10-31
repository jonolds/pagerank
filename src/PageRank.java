import scala.Tuple2;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@SuppressWarnings("unused")
public class PageRank {

	private static ArrayList<Double> r = new ArrayList<>();
	private static final double beta = 0.8;
	private static final int MAX_ITER = 40;

	static void pageRank() throws Exception {

	    JavaRDD<String> graph = settings().read().textFile("graph.txt").toJavaRDD();
	    JavaPairRDD<Integer, Integer> edges = graph.mapToPair(e -> getEdge(e));
	    JavaPairRDD<List<Integer>, Double> matrix = edges.distinct().groupByKey().flatMapToPair(p -> getDegree(p));

	    int n = (int) edges.groupByKey().count();

	    // TODO Initialize vector r

	    for (int i=0; i<MAX_ITER; i++) {

	    	// TODO Compute the new vector r and replace the old r with the new one.

	    }

	    int[] sortedOrder = sort(new ArrayList<Double>(r));

	    // Top 5 nodes with highest page rank
	    System.out.println(sortedOrder[n-1]+1+": "+r.get(sortedOrder[n-1]));
	    System.out.println(sortedOrder[n-2]+1+": "+r.get(sortedOrder[n-2]));
	    System.out.println(sortedOrder[n-3]+1+": "+r.get(sortedOrder[n-3]));
	    System.out.println(sortedOrder[n-4]+1+": "+r.get(sortedOrder[n-4]));
	    System.out.println(sortedOrder[n-5]+1+": "+r.get(sortedOrder[n-5]));
	    // Top 5 nodes with lowest page rank
	    System.out.println(sortedOrder[0]+1+": "+r.get(sortedOrder[0]));
	    System.out.println(sortedOrder[1]+1+": "+r.get(sortedOrder[1]));
	    System.out.println(sortedOrder[2]+1+": "+r.get(sortedOrder[2]));
	    System.out.println(sortedOrder[3]+1+": "+r.get(sortedOrder[3]));
	    System.out.println(sortedOrder[4]+1+": "+r.get(sortedOrder[4]));
	}

	// Selection sort. Return a list of indices in the ascending order.
	private static int[] sort(ArrayList<Double> arr) {
		int[] order = new int[arr.size()];
		for (int i=0; i<arr.size(); i++) {
			order[i] = i;
		}

		for (int i = 0; i < arr.size() - 1; i++) {
			int index = i;
			for (int j = i + 1; j < arr.size(); j++)
				if (arr.get(j) < arr.get(index))
					index = j;

			double smallerNumber = arr.get(index);
			arr.set(index, arr.get(i));
			arr.set(i, smallerNumber);

			int smallerIndex = order[index];
			order[index] = order[i];
			order[i] = smallerIndex;
		}
		return order;
	}

	private static Iterator<Tuple2<List<Integer>, Double>> getDegree(Tuple2<Integer, Iterable<Integer>> p) {
		// TODO Compute the values in matrix, which are given by 1/deg(i)

		return null;
	}

	private static Tuple2<Integer, Integer> getEdge(String e) {
		String[] edge = e.split("\t");
		return new Tuple2<>(Integer.parseInt(edge[0]), Integer.parseInt(edge[1]));
	}

	static SparkSession settings() throws IOException {
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);
		SparkSession.clearActiveSession();
		SparkSession spark = SparkSession.builder().appName("Kmeans").config("spark.master", "local").config("spark.eventlog.enabled","true").config("spark.executor.cores", "2").getOrCreate();
		SparkContext sc = spark.sparkContext();
		sc.setLogLevel("WARN");
		FileUtils.deleteDirectory(new File("output"));
		return spark;
	}
	public static void main2(String[] args) throws Exception {
		pageRank();
		Thread.sleep(20000);
	}
}