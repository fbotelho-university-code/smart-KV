/**
 * 
 */
package smartkv.client;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import smartkv.server.MapSmart;

/**
 * @author fabiim
 *
 */
public class KeyValueDatastoreProxyTest {
	//TODO - laun smart, and maybe do it in verbose mode....
	//XXX maybe set up tables for each method dynamically. 
	
	
	public static  KeyValueDatastoreProxy ds;
	private byte[] key_1 = "1".getBytes(); 
	private byte[] value_1 = "1".getBytes(); 
	private byte[] key_2 = "2".getBytes(); 
	private byte[] value_2 = "2".getBytes(); 
	
	@BeforeClass 
	public static void startup(){
		ds = new KeyValueProxy(0);
		System.out.println("here");
	
	}
	
	@Before 
	public void initTest(){
		ds.clear();
	}

	
	/**
	 * Test method for {@link smartkv.client.KeyValueDatastoreProxy#put(java.lang.String, byte[], byte[])}.
	 */
	@Test
	public void testPut() {
		String tableName = "put";
		ds.createTable(tableName);
		DatastoreValue val = ds.put(tableName,key_1, value_1);
		assertNull(val); // previous value was null. 
		assertNull(val.data); // previous value was null. 
		val = ds.put(tableName,key_1, value_2); //replace value, and get previous value. 
		assertNotNull(val);  //previous value is not null (should be value_1) 
		assertArrayEquals(val.data, value_1); // previous value was value_1
		DatastoreValue ds2 = ds.get(tableName,key_1); 
		assertNotNull(ds2);
		assertNotNull(ds2.data);
		Assert.assertArrayEquals(ds2.data, value_2); // should get value 2 
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
		assertArrayEquals(ds.get(tableName, key_1).data, value_1); 
		inserted = ds.insert(tableName,key_1, value_2);
		assertTrue(inserted); 
		assertArrayEquals(ds.get(tableName, key_1).data, value_2); //value has been replaced.  
	}

	/**
	 * Test method for {@link smartkv.client.KeyValueDatastoreProxy#get(java.lang.String, byte[])}.
	 */
	@Test
	public void testGet() {
		String tableName = "get";
		ds.createTable(tableName);
		DatastoreValue val = ds.get(tableName, key_1);
		assertNull(val); // there is no key_1 yet.
		assertNull(val.data); // there is no key_1 yet.
		ds.put(tableName,key_1, value_1);
		val = ds.get(tableName, key_1);
		assertNull(ds.remove(tableName, key_2)); //entry does not exists.
		assertArrayEquals(val.data, value_1); 
	}
	

	/**
	 * Test method for {@link smartkv.client.KeyValueDatastoreProxy#remove(java.lang.String, byte[])}.
	 */
	@Test
	public void testRemoveStringByteArray() {
		String tableName = "removeStringByteArray"; 
		ds.createTable(tableName);
		ds.put(tableName, key_1, value_1);
		DatastoreValue val =ds.remove(tableName, key_1);
		assertNotNull(val);
		assertArrayEquals(val.data, value_1); 
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
		assertArrayEquals(ds.get(tableName, key_1).data, value_2); 
		assertTrue(ds.replace(tableName, key_1, value_2, value_1));
		assertArrayEquals(ds.get(tableName, key_1).data, value_1);
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
		assertArrayEquals(ds.get(tableName, key_1).data, value_1); 
		assertArrayEquals(ds.putIfAbsent(tableName, key_1, value_2).data, value_1);
		assertArrayEquals(ds.putIfAbsent(tableName, key_1, value_2).data, value_1);
	}

	/**
	 * Test method for {@link smartkv.client.TableDataStoreProxy#clear()}.
	 */
	@Test
	public void testClear() {
		ds.clear(); 
		assertFalse(ds.containsTable("ola"));
		ds.createTable("ola");
		ds.put("ola", new byte[10], new byte[10]);
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
		ds.put(tableName,"c".getBytes(), "c".getBytes());  //Removes the eldest element a since max size is 2. 
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
		ds.remove("size", key_2);
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
		ds.put(tableName, "1".getBytes(), value_1);
		//FIXME: document this behaviour - put override column name... 
		assertTrue(Arrays.equals(ds.get(tableName, "1".getBytes()).data, value_1));
	}
	
	@Test
	public void testValues(){
		String tableName = "values"; 
		ds.createTable(tableName);
		
		ds.put(tableName, key_1, value_1); 
		ds.put(tableName, key_2, value_2); 
		Collection<DatastoreValue> maps = ds.values(tableName);
		
		assertSame(maps.size(), 2);
		boolean c1=false,c2=false;
		for (DatastoreValue m : maps){
			if (Arrays.equals(m.data, value_1)){
				c1 = true;
			}
			else if (Arrays.equals(m.data, value_2)){
				c2 = true;
			}
		}
		if (!(c1 && c2)){
			fail("Not in return");
		}
	}
}
