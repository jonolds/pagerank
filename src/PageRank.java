import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import com.twitter.chill.java.ArraysAsListSerializer;

import pagerank.Print;
import scala.Tuple2;

@SuppressWarnings("unused")
public class PageRank {
	static double[] r;
	static final double beta = 0.8;
	static final int MAX_ITER = 40;

	public static void pagerank(SparkSession ss) throws Exception {
		JavaPairRDD<Integer, Integer> edges = initGraph(ss, "graph_tiny.txt").distinct();
		JavaPairRDD<Integer, Iterable<Integer>> grouped = edges.groupByKey().sortByKey().cache();



		JavaPairRDD<Tuple2<Integer, Iterable<Integer>>, Long> indexed1 = grouped.zipWithIndex();
		final int n = (int) grouped.count();
		final Hashtable<Integer, Integer> ht = getHashtable(indexed1.mapToPair(x->new Tuple2<>(x._1._1, x._2)).collect());

		JavaPairRDD<Integer, Iterable<Integer>> indexed = indexed1.mapToPair(x->new Tuple2<>(Math.toIntExact(x._2), x._1._2));


		class DegreeFunc implements PairFunction<Tuple2<Integer, Iterable<Integer>>, Integer, List<Double>> {
			public Tuple2<Integer, List<Double>> call(Tuple2<Integer, Iterable<Integer>> t) throws Exception {
				List<Double> arr = new ArrayList<>(Collections.nCopies(n, 0.0));
				List<Integer> list = StreamSupport.stream(t._2.spliterator(), false).collect(Collectors.toList());
				int count = list.size();
				for(Integer i : list)
					arr.set(ht.get(i), 1.0/count);
				return new Tuple2<>(t._1, arr);
			}
		}

		JavaPairRDD<Integer, List<Double>> matrix = indexed.mapToPair(new DegreeFunc());





		grouped.saveAsTextFile("output/out1");
		matrix.saveAsTextFile("output/out2");


		r = new double[n];

		Arrays.fill(r, 1.0/n);

		List<Tuple2<Integer, List<Double>>> mat = matrix.collect();

//		List<Tuple2<List<Integer>, Double>> list = matrix.collect();
//		Print.commaTup(list);





		for (int k=0; k<2; k++) {
			// TODO Compute the new vector r and replace the old r with the new one.
			double[] new_r = new double[n];

			System.out.print("Starting #" + k + "\n r: ");
			Print.comma(r);
			System.out.print("r2: ");
			Print.comma(new_r);

			for(int z = 0; z < n; z++) {
				Double[] ints = mat.get(z)._2.toArray(new Double[mat.get(z)._2.size()]);
				for(int i = 0; i < r.length; i++) {
					System.out.println("z: " + z + "  i: " + i);
					new_r[i] += r[z]*ints[i];
				}
			}
			for(int i = 0; i < r.length; i++)
				r[i] = new_r[i];
			System.out.print("r: "); Print.comma(r);
			System.out.println("Ending #" + k + "\n\n");
		}

//		int[] sortedOrder = sort(copy(r));
	}

/* getDegree() */
	static Iterator<Tuple2<List<Integer>, Double>> getDegree(Tuple2<Integer, Iterable<Integer>> p) {
		List<Tuple2<List<Integer>, Double>> list = new ArrayList<>();
		List<Integer> list_ints = new ArrayList<>(Arrays.asList(p._1));
		p._2.forEach(x->list_ints.add(x));
		p._2.forEach(x->list.add(new Tuple2<>(list_ints, 1.0)));
		return list.iterator();
	}

	static Tuple2<List<Integer>, Double> getDegree2(Tuple2<Integer, Iterable<Integer>> p) {
		List<Integer> list_ints = new ArrayList<>(Arrays.asList());
		p._2.forEach(x->list_ints.add(x));
		Collections.sort(list_ints);
		return new Tuple2<>(list_ints, 1.0/list_ints.size());
	}

/* calcRow() */
	static Double calcRow(Tuple2<List<Integer>, Double> p, ArrayList<Double> r) {
		Double sum = 0.0;
		List<Integer> l2 = p._1.subList(1, p._1.size()-1);
		for(int i = 0; i < l2.size(); i++)
			sum += l2.get(i);
		return sum;
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
	static void printOut(int[] sortedOrder, int n, ArrayList<Double> r) {
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

	static Hashtable<Integer, Integer> getHashtable(List<Tuple2<Integer, Long>> list) {
		Hashtable<Integer, Integer> ht = new Hashtable<>(list.size());
		for(int i = 0; i < list.size(); i++)
			ht.put(list.get(i)._1, Math.toIntExact(list.get(i)._2));
		return ht;
	}

/* COMPARE */
	static class ListComp implements Comparator<List<Integer>>, Serializable {
		public int compare(List<Integer> a, List<Integer> b) {
			return (a.get(0) > b.get(0)) ? 1 : (a.get(0) < b.get(0)) ? -1 : 0;
		}
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