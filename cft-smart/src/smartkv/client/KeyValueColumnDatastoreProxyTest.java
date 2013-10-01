/**
 * 
 */
package smartkv.client;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * @author fabiim
 *
 */
public class KeyValueColumnDatastoreProxyTest {
	//TODO - laun smart, and maybe do it in verbose mode....
	//XXX maybe set up tables for each method dynamically. 
	
	
	public static  KeyValueColumnDatastoreProxy ds;
	private byte[] key_1 = "1".getBytes(); 
	private TreeMap<String,byte[]> value_1 = Maps.newHashMap(ImmutableMap.of("1", "1".getBytes(), "2", "2".getBytes()));
	private byte[] key_2 = "2".getBytes(); 
	private TreeMap<String,byte[]> value_2 = Maps.newHashMap(ImmutableMap.of("4", "4".getBytes(), "3", "3".getBytes()));
	private byte[] key_3 = "3".getBytes(); 
	private TreeMap<String,byte[]> value_3 = Maps.newHashMap(ImmutableMap.of("5", "5".getBytes(), "6", "6".getBytes()));
	
	@BeforeClass 
	public static void startup(){
		ds = new ColumnProxy(0);
		
	}
	
	@Before 
	public void initTest(){
		ds.clear();
	}
	
	@Test
	public void testSetColumn(){
		String tableName = "setColumn";
		
		assertFalse(ds.setColumn(tableName, key_2, "whatever", new byte[1])); //table does not exits.
		ds.createTable(tableName);
		assertFalse(ds.setColumn(tableName, key_2, "whatever", new byte[1])); //key does not exist
		ds.put(tableName, key_1, value_1);
		assertFalse(ds.setColumn(tableName, key_2, "whatever", new byte[1])); //key (column) does not exist
		String k_to_change = value_1.keySet().iterator().next();
		assertTrue(ds.setColumn(tableName, key_1, k_to_change, "weChangedThisValue".getBytes()));
		TreeMap<String,byte[]> val = ds.getValue(tableName, key_1); 
		assertTrue(Arrays.equals(val.get(k_to_change), "weChangedThisValue".getBytes()));
	}
	
	@Test 
	public void testGetColumn(){
		String tableName = "getColumn";
		ds.createTable(tableName);
		TreeMap<String,byte[]>map  = Maps.newTreeMap(ImmutableMap.of("1" ,"1".getBytes()));
		ds.put(tableName, key_1, map);
		assertTrue(Arrays.equals(ds.getColumn(tableName, key_1, "1" ),"1".getBytes()));
		ds.setColumn(tableName, key_1, "1","2".getBytes());
		assertTrue(Arrays.equals(ds.getColumn(tableName, key_1, "1"),"2".getBytes()));
	}
	
	/**
	 * Test method for {@link smartkv.client.KeyValueDatastoreProxy#put(java.lang.String, byte[], byte[])}.
	 */
	@Test
	public void testPut() {
		String tableName = "put";
		ds.createTable(tableName);
		TreeMap<String,byte[]> val = ds.put(tableName,key_1, value_1);
		assertNull(val); // previous value was null. 
		val = ds.put(tableName,key_1, value_2); //replace value, and get previous value. 
		assertNotNull(val);  //previous value is not null (should be value_1)
		assertMapAreEqual(val,value_1); 
		assertMapAreEqual(ds.getValue(tableName, key_1), value_2); 
	}

	private boolean areByteArrayValueMapEqual(TreeMap<?, byte[]> a,
			TreeMap<?, byte[]> b) {
		if (a.size() == b.size() ){
			for (Entry<?, byte[]>  en : a.entrySet()){
				if (b.containsKey(en.getKey()) && Arrays.equals(en.getValue(), b.get(en.getKey()))){
					continue;
				}
				return false; 
			}
			return true; 
		}
		return false; 
	}
	
	
	private  void assertMapAreEqual(final TreeMap<?,byte[]> a , final TreeMap<?,byte[]> b){
		assertTrue(areByteArrayValueMapEqual(a,b));
	}
	/**
	 * Test method for {@link smartkv.client.KeyValueDatastoreProxy#insert(java.lang.String, byte[], byte[])}.
	 */
	@Test
	public void testInsert() {
		String tableName = "insert";
		assertFalse(ds.insert("nonExistent", key_1, value_1)); // can not insert in nonExistent table.
		ds.createTable(tableName);
		boolean inserted = ds.insert(tableName,key_1, value_1);
		assertTrue(inserted);
		assertMapAreEqual(ds.getValue(tableName, key_1), value_1); 
		inserted = ds.insert(tableName,key_1, value_2);
		assertTrue(inserted); 
		assertMapAreEqual(ds.getValue(tableName, key_1), value_2); //value has been replaced.  
	}

	/**
	 * Test method for {@link smartkv.client.KeyValueDatastoreProxy#get(java.lang.String, byte[])}.
	 */
	@Test
	public void testGet() {
		String tableName = "get";
		ds.createTable(tableName);
		TreeMap<String,byte[]> val = ds.getValue(tableName, key_1);
		assertNull(val); // there is no key_1 yet.
		ds.put(tableName,key_1, value_1);
		val = ds.getValue(tableName, key_1);
		assertNull(ds.removeValue(tableName, key_2)); //entry does not exists.
		for (Entry<String, byte[]> m : val.entrySet()){
			System.out.println(m.getKey() + "-" + Arrays.toString(m.getValue()));
			System.out.println( m.getKey() + "-" + Arrays.toString(value_1.get(m.getKey())));
		}
		assertMapAreEqual(val, value_1); 
	}
	

	/**
	 * Test method for {@link smartkv.client.KeyValueDatastoreProxy#remove(java.lang.String, byte[])}.
	 */
	@Test
	public void testRemoveStringByteArray() {
		String tableName = "removeStringByteArray"; 
		ds.createTable(tableName);
		ds.put(tableName, key_1, value_1);
		TreeMap<String,byte[]> val =ds.removeValue(tableName, key_1);
		assertNotNull(val);
		assertMapAreEqual(val, value_1); 
	}
	
	/**
	 * Test method for {@link smartkv.client.KeyValueDatastoreProxy#replace(java.lang.String, byte[], byte[], byte[])}.
	 */
	@Test
	public void testReplace() {
		String tableName ="replace";
		ds.createTable("replace"); 
		assertFalse(ds.replace(tableName, key_1, value_1, value_2));
		assertFalse(ds.replace(tableName, key_1, value_1, value_2)); 
		assertFalse(ds.containsKey(tableName, key_1));
		ds.put(tableName, key_1, value_1);
		assertTrue(ds.replace(tableName, key_1, value_1, value_2));
		assertMapAreEqual(ds.getValue(tableName, key_1), value_2); 
		assertTrue(ds.replace(tableName, key_1, value_2, value_1));
		assertMapAreEqual(ds.getValue(tableName, key_1), value_1);
	}

	/**
	 * Test method for {@link smartkv.client.KeyValueDatastoreProxy#remove(java.lang.String, byte[], byte[])}.
	 */
	@Test
	public void testRemoveStringByteArrayByteArray() {
		String tableName = "atomicRemove"; 
		ds.createTable(tableName); 
		
		assertFalse(ds.remove(tableName,key_1, value_1)); //should not remove, key_1 is not mapped to  nothing
		assertFalse(ds.remove(tableName,key_1, value_1));// still.. does not remove. 
		assertFalse(ds.containsKey(tableName,key_1)); //should not contain
		
		ds.put(tableName,key_1, value_1);
		assertTrue(ds.remove(tableName,key_1, value_1)); 
		assertFalse(ds.containsKey(tableName,key_1));
	}

	/**
	 * Test method for {@link smartkv.client.KeyValueDatastoreProxy#putIfAbsent(java.lang.String, byte[], byte[])}.
	 */
	@Test
	public void testPutIfAbsent() {
		String tableName = "putIfAbset"; 
		ds.createTable(tableName); 
		assertNull(ds.putIfAbsent(tableName, key_1, value_1));
		assertTrue(ds.containsKey(tableName, key_1));
		assertMapAreEqual(ds.getValue(tableName, key_1), value_1); 
		assertMapAreEqual(ds.putIfAbsent(tableName, key_1, value_2), value_1);
		assertMapAreEqual(ds.putIfAbsent(tableName, key_1, value_2), value_1);
	}

	/**
	 * Test method for {@link smartkv.client.TableDataStoreProxy#clear()}.
	 */
	@Test
	public void testClear() {
		ds.clear(); 
		assertFalse(ds.containsTable("ola"));
		ds.createTable("ola");
		ds.put("ola", key_1, value_1);
		ds.clear();
		assertFalse(ds.containsKey("ola", new byte[10])); 
		///XXX you can do better
	}

	/**
	 * Test method for {@link smartkv.client.TableDataStoreProxy#createTable(java.lang.String)}.
	 */
	@Test
	public void testCreateTableString() {
		assertTrue(ds.createTable("createTable")); 
		assertFalse(ds.createTable("createTable")); 
	}

	/**
	 * Test method for {@link smartkv.client.TableDataStoreProxy#createTable(java.lang.String, long)}.
	 */
	@Test
	public void testCreateTableStringLong() {
		String tableName="table_created";
		assertTrue(ds.createTable(tableName,2));  
		assertFalse(ds.createTable(tableName,2)); //table already existed. Should not allow creation.
		ds.put(tableName, key_1, value_1); 
		ds.put(tableName, key_2, value_2); 
		assertTrue(ds.containsKey(tableName,key_1)); 
		assertTrue(ds.containsKey(tableName,key_2));
		ds.put(tableName,key_3, value_3);  //Removes the eldest element a since max size is 2. 
		assertFalse(ds.containsKey(tableName, key_1));
		assertTrue(ds.containsKey(tableName, key_2));
		assertTrue(ds.containsKey(tableName, "c".getBytes()));
	}

	/**
	 * Test method for {@link smartkv.client.TableDataStoreProxy#removeTable(java.lang.String)}.
	 */
	@Test
	public void testRemoveTable() {
		assertFalse(ds.removeTable("this_table_does_not_exists")); 
		assertTrue(ds.createTable("table_to_remove"));
		assertTrue(ds.removeTable("table_to_remove"));
		
	}

	/**
	 * Test method for {@link smartkv.client.TableDataStoreProxy#containsTable(java.lang.String)}.
	 */
	@Test
	public void testContainsTable() {
		assertTrue(ds.createTable("test_contains"));
		assertFalse(ds.containsTable("this_table_does_not_exists")); 
		assertTrue(ds.containsTable("test_contains")); 
	}

	/**
	 * Test method for {@link smartkv.client.TableDataStoreProxy#clear(java.lang.String)}.
	 */
	@Test
	public void testClearString() {
		String tableName = "test_clear"; 
		assertTrue(ds.createTable(tableName));
		assertTrue(ds.insert(tableName,key_1,value_1));
		Assert.assertEquals(ds.size(tableName),  1);
		ds.clear(tableName); 
		Assert.assertEquals((int) ds.size(tableName),  0);
	}

	/**
	 * Test method for {@link smartkv.client.TableDataStoreProxy#isEmpty(java.lang.String)}.
	 */
	@Test
	public void testIsEmpty() {
		String tableName= "testIsEmptyString";
		ds.createTable(tableName); 
		assertTrue(ds.isEmpty(tableName));
		ds.put(tableName,key_1, value_1); 
		assertFalse(ds.isEmpty(tableName)); 

	}

	/**
	 * Test method for {@link smartkv.client.TableDataStoreProxy#size(java.lang.String)}.
	 */
	@Test
	public void testSize() {
		ds.createTable("size");
		
		assertEquals(ds.size("size"), 0);
		ds.put("size",key_1, value_1);
		assertEquals(ds.size("size"), 1);
		ds.put("size", key_2, value_1);
		assertEquals(ds.size("size"), 2);
		ds.removeValue("size", key_2);
		assertEquals(ds.size("size"), 1);
		ds.clear("size");
		assertEquals(ds.size("size"), 0);

	}

	/**
	 * Test method for {@link smartkv.client.TableDataStoreProxy#containsKey(java.lang.String, byte[])}.
	 */
	@Test
	public void testContainsKey() {
		String tableName = "testContainsKey"; 
		ds.createTable(tableName); 
		assertFalse(ds.containsKey(tableName, key_1));
		ds.put(tableName,key_1, value_1);
		assertTrue(ds.containsKey(tableName, key_1));
	}
	
	/**
	 * Test method for {@link smartkv.client.TableDataStoreProxy#getAndIncrement(java.lang.String, java.lang.String)}.
	 */
	@Test
	public void testGetAndIncrement() {
		String tableName = "getAndIncrement";
		ds.createTable(tableName);
		int i = ds.getAndIncrement(tableName, "1");
		assertEquals(i,0);
		i = ds.getAndIncrement(tableName, "1");
		assertEquals(i,1);
		//ds.put(tableName, "1".getBytes(), value_1);
		//FIXME: document this behaviour - put override column name... 
		//assertMapAreEqual(ds.getValue(tableName, "1".getBytes()), value_1);
	}
	
	@Test
	public void testValues(){
		String tableName = "values"; 
		ds.createTable(tableName);
		
		ds.put(tableName, key_1, value_1); 
		ds.put(tableName, key_2, value_2); 
		ds.put(tableName, key_3, value_3);
		Collection<TreeMap<String,byte[]>> maps = ds.valueS(tableName);
		
		assertSame(maps.size(), 3);
		boolean c1=false,c2=false,c3=false;
		for (TreeMap<String,byte[]> m : maps){
			if (areByteArrayValueMapEqual(m, value_1)){
				c1 = true;
			}
			else if (areByteArrayValueMapEqual(m, value_2)){
				c2 = true;
			}
			else if (areByteArrayValueMapEqual(m, value_3)){
				c3 = true; 
			}
		}
		if (!(c1 && c2&& c3)){
			fail("Not in map");
		}
	}
}
