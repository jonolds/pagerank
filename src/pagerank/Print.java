package pagerank;

import java.util.List;

public class Print {
	public static <T>void comma(List<T> l) {
		if(!l.isEmpty()) {
			System.out.print(l.get(0));
			if(l.size() > 1)
				for(int i = 1; i < l.size(); i++)
					System.out.print(", " + l.get(i));
			System.out.println();
		}
	}

	public static <T>void comma(T[] arr) {
		if(arr.length > 0) {
			System.out.print(arr[0]);
			if(arr.length > 1)
				for(int i = 1; i < arr.length; i++)
					System.out.print(", " + arr[i]);
			System.out.println();
		}
	}

	public static void comma(int[] arr) {
		if(arr.length > 0) {
			System.out.print(arr[0]);
			if(arr.length > 1)
				for(int i = 1; i < arr.length; i++)
					System.out.print(", " + arr[i]);
			System.out.println();
		}
	}

	public static <T>void lns(List<T> l) {
		for(int i = 0; i < l.size(); i++)
			System.out.println(l.get(i));
	}

	public static <T>void commaOrdered(List<T> l, int[] order) {
		if(!l.isEmpty() && order.length == l.size()) {
			System.out.print(l.get(order[0]));
			if(l.size() > 1)
				for(int i = 1; i < l.size(); i++)
					System.out.print(", " + l.get(order[i]));
			System.out.println();
		}
	}
}
