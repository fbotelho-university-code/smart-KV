package smartkv.server.util;

import java.util.Map;

public class TestLru {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Map<String,String> test = new LRULinkedHashMap<String,String>(2); 
		test.put("1", "1"); 
		test.put("2", "2"); 
		test.put("3", "3"); 
		System.out.println(test.size());
		for (String k : test.keySet()){
			System.out.println(k);
		}
		System.out.println(test.containsKey("1"));
	}

}
