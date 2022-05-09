package Spark;

import java.io.Serializable;
import java.util.Comparator;



public class TupleSorter implements Serializable, Comparator<Integer> {

	private static final long serialVersionUID = 1L;

	public int compare(Integer o1, Integer o2) {
		if (o1< o2)
			return 1;
		else if (o1 >o2)
			return -1;
		else 
			return 0;
	}

}

