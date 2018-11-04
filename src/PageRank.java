import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import pagerank.Print;
import scala.Tuple2;

@SuppressWarnings("unused")
public class PageRank {

	static ArrayList<Double> r;
	static final double beta = 0.8;
	static final int MAX_ITER = 40;

	public static void pagerank(SparkSession ss) throws Exception {

		JavaPairRDD<Integer, Integer> edges = initGraph(ss, "graph_tiny.txt");
		JavaPairRDD<Integer, Iterable<Integer>> grouped = edges.distinct().groupByKey().sortByKey();
//		JavaPairRDD<List<Integer>, Double> matrix = grouped.flatMapToPair(p -> getDegree(p));

		JavaPairRDD<List<Integer>, Double> matrix = grouped.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Iterable<Integer>>, List<Integer>, Double>() {
			public Iterator<Tuple2<List<Integer>, Double>> call(Tuple2<Integer, Iterable<Integer>> t) throws Exception {
				List<Tuple2<List<Integer>, Double>> list = new ArrayList<>();
				List<Integer> l2 = new ArrayList<>();
				t._2.forEach(x->l2.add(x));
				list.add(new Tuple2<>(l2, 1.0));

				return list.iterator();
			}

		});


//		JavaPairRDD<List<Integer>, Double> matrix = matrix1.reduceByKey((v1, v2)->v1+v2).mapToPair(x->new Tuple2<>(x._1, 1.0/x._2));

		grouped.saveAsTextFile("output/out1");
		matrix.saveAsTextFile("output/out2");
		int n = (int) edges.groupByKey().count();
		System.out.println(n);

		r = new ArrayList<>(Collections.nCopies(n, Double.valueOf(1)/n));

		for (int i=0; i<MAX_ITER; i++) {
			// TODO Compute the new vector r and replace the old r with the new one.
		}


//		int[] sortedOrder = sort(copy(r));
	}

	static Iterator<Tuple2<List<Integer>, Double>> getDegree(Tuple2<Integer, Iterable<Integer>> p) {
		List<Integer> list_int = new ArrayList<>();
		Iterator<Integer> it = p._2.iterator();
		while(it.hasNext())
			list_int.add(it.next());

		List<Tuple2<List<Integer>, Double>> list = new ArrayList<>();
		for(Integer i : list_int)
			list.add(new Tuple2<>(list_int, 1.0));
		return list.iterator();
	}

	// Selection sort. Return a list of indices in the ascending order.
	static int[] sort(ArrayList<Double> arr) {
		int[] order = new int[arr.size()];
		for (int i=0; i<arr.size(); i++)
			order[i] = i;

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





/* Print Output */
	static void printOut(int[] sortedOrder, int n) {
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


/* Initialize Graph */
	static JavaPairRDD<Integer, Integer> initGraph(SparkSession ss, String filename) {
		JavaRDD<String> graph = ss.read().textFile(filename).javaRDD();
		return graph.mapToPair(e -> getEdge(e));
	}

	static Tuple2<Integer, Integer> getEdge(String e) {
		String[] edge = e.split("\t");
		return new Tuple2<>(Integer.parseInt(edge[0]), Integer.parseInt(edge[1]));
	}

/* Copy centroid (DEEP COPY) */
	static <T>ArrayList<T> copy(ArrayList<T> cent) {
		return (ArrayList<T>)cent.stream().collect(Collectors.toList());
	}

/* Main / Standard Setup */
	public static void main(String[] args) throws Exception {
		SparkSession ss = settings();
		pagerank(ss);
//		Thread.sleep(20000);
		ss.close();
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
}