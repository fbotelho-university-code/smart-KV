/**
 * 
 */
package tirateimas;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 *Convince me that the with LRU (LinkedHashMap) the put does not affect order. 
 *
 */
public class LRU {
	
	public static class LRULinkedHashMap<K, V> extends LinkedHashMap<K, V> {
	    private static final long serialVersionUID = -2964986094089626647L;
	    protected int maximumCapacity;

	    public LRULinkedHashMap(int initialCapacity, int maximumCapacity) {
	        super(initialCapacity, 0.75f, true);
	        this.maximumCapacity = maximumCapacity;
	    }

	    public LRULinkedHashMap(int maximumCapacity) {
	        super(16, 0.75f, true);
	        this.maximumCapacity = maximumCapacity;
	    }

	    @Override
	    protected boolean removeEldestEntry(java.util.Map.Entry<K, V> eldest) {
	        if (this.size() > maximumCapacity)
	            return true;
	        return false;
	    }
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Map<Integer, Integer> lru = new LRULinkedHashMap<Integer,Integer>(0, 5);
		
		lru.put(1, 1); 
		lru.put(2, 2); 
		lru.put(1, 1); 
		lru.put(3, 3); 
		lru.put(1, 1);
		
		lru.put(4, 4);
		System.out.println(lru);
		lru.get(2); 
		lru.put(1, 1); 
		lru.put(5, 5);
		System.out.println(lru); 

		lru.put(6, 6);
		
		System.out.println(lru); 
	}

}
