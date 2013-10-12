/**
 * 
 */
package smartkv.client.tables;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import smartkv.client.ColumnProxy;
import smartkv.client.util.JavaSerializer;
import smartkv.client.util.UnsafeJavaSerializer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * @author fabiim
 *
 */
public class ColumnTableTest {
	ValueObject value_1 = new ValueObject(1);
	KeyObject key_1 = new KeyObject(1); 
	
	ValueObject value_2 = new ValueObject(2);
	KeyObject key_2 = new KeyObject(2); 
	
	ValueObject value_3 = new ValueObject(3);
	KeyObject key_3 = new KeyObject(3);
		
	public static class KeyObject implements Serializable{
		public final int id;
		public KeyObject(int id){
			this.id = id; 
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + id;
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			KeyObject other = (KeyObject) obj;
			if (id != other.id)
				return false;
			return true;
		}
	}
	
	
	
	private ColumnTable<KeyObject, ValueObject> ds;
	public static int id=0; 
	public ColumnTableTest(){
		  ds =ColumnTable_.<KeyObject, ValueObject>getTable(new ColumnProxy(id++), "ttestTable", JavaSerializer.<KeyObject>getJavaSerializer(), AnnotatedColumnObject.newAnnotatedColumnObject(ValueObject.class)); 
	}
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		
	}

	@Before 
	public void initTest(){
		ds.clear();
	}
	
	/**
	 * Test method for {@link smartkv.client.tables.ColumnTable#getColumn(java.lang.Object, java.lang.String)}.
	 */
	@Test
	public void testGetColumn(){
		ds.put(key_1, value_1);
		assertEquals(ds.getColumn(key_1, "getCenas"), "1"); 
		ds.setColumn(key_1, "getCenas", "2");
		assertEquals(ds.getColumn(key_1, "getCenas"),"2"); 
	}
	
	/**
	 * Test method for {@link smartkv.client.tables.ColumnTable#setColumn(java.lang.Object, java.lang.String, java.lang.Object)}.
	 */
	@Test
	public void testSetColumn(){
		assertFalse(ds.setColumn( key_2, "whatever", new byte[1])); //key does not exist
		ds.put( key_1, value_1);
		assertTrue(ds.setColumn(key_1, "getId", 2));
		assertFalse(ds.setColumn( key_1, "whatever", new byte[1])); //key (column) does not exist
		assertEquals(ds.get(key_1).getId(), 2);
	}

	/**
	 * Test method for {@link smartkv.client.tables.IKeyValueTable#remove(java.lang.Object, java.lang.Object)}.
	 */
	@Test
	public void testRemoveKV() {
		ds.put(key_1, value_1);
		ValueObject val=ds.remove(key_1);
		assertNotNull(val);
		assertEquals(val, value_1); 
		
	}

	/**
	 * Test method for {@link smartkv.client.tables.IKeyValueTable#putIfAbsent(java.lang.Object, java.lang.Object)}.
	 */
	@Test
	public void testPutIfAbsent() {
		assertNull(ds.putIfAbsent( key_1, value_1));
		assertTrue(ds.containsKey( key_1));
		assertEquals(ds.get( key_1), value_1); 
		assertEquals(ds.putIfAbsent( key_1, value_2), value_1);
		assertEquals(ds.putIfAbsent( key_1, value_2), value_1);
		
	}

	/**
	 * Test method for {@link smartkv.client.tables.IKeyValueTable#remove(java.lang.Object)}.
	 */
	@Test
	public void testRemoveK() {
		ds.put( key_1, value_1);
		ValueObject val =ds.remove(key_1);
		assertNotNull(val);
		assertEquals(val, value_1); 
	}

	/**
	 * Test method for {@link smartkv.client.tables.IKeyValueTable#put(java.lang.Object, java.lang.Object)}.
	 */
	@Test
	public void testPut() {
		ValueObject val = ds.put(key_1, value_1);
		assertNull(val); // previous value was null. 
		val = ds.put(key_1, value_2); //replace value, and get previous value. 
		assertNotNull(val);  //previous value is not null (should be value_1)
		assertEquals(val,value_1); 
		assertEquals(ds.get( key_1), value_2);
		
	}

	/**
	 * Test method for {@link smartkv.client.tables.IKeyValueTable#get(java.lang.Object)}.
	 */
	@Test
	public void testGet() {
		ValueObject val = ds.get( key_1);
		assertNull(val); // there is no key_1 yet.
		ds.put(key_1, value_1);
		val = ds.get( key_1);
		assertNull(ds.remove( key_2)); //entry does not exists.
	
		assertEquals(val, value_1); 
	}

	/**
	 * Test method for {@link smartkv.client.tables.ITable#clear()}.
	 */
	@Test
	public void testClear() {
		ds.put(key_1, value_1);
		ds.clear(); 
		assertEquals(ds.size(),0);
	}

	/**
	 * Test method for {@link smartkv.client.tables.ITable#containsKey(java.lang.Object)}.
	 */
	@Test
	public void testContainsKey() {
		assertFalse(ds.containsKey( key_1));
		ds.put(key_1, value_1);
		assertTrue(ds.containsKey(key_1));
		
	}


	/**
	 * Test method for {@link smartkv.client.tables.ITable#isEmpty()}.
	 */
	@Test
	public void testIsEmpty() {
		ds.put(key_1, value_1);
		assertFalse(ds.isEmpty());
		ds.clear(); 
		assertTrue(ds.isEmpty()); 
	}



	/**
	 * Test method for {@link smartkv.client.tables.ITable#size()}.
	 */
	@Test
	public void testSize() {
		ds.put(key_1, value_1);
		assertEquals(ds.size(), 1);
		ds.clear(); 
		assertEquals(ds.size(), 0); 
	}


	/**
	 * Test method for {@link smartkv.client.tables.ITable#getAndIncrement(java.lang.String)}.
	 */
	@Test
	public void testGetAndIncrement() {
		int i = ds.getAndIncrement( "1");
		assertEquals(i,0);
		i = ds.getAndIncrement("1");
		assertEquals(i,1);
		
	}

}
