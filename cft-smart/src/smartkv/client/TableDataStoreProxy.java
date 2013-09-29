
package smartkv.client;



/**
 * A DataStoreProxy is a proxy object that  knows how to communicate with the data store. The proxy implements all the data store supported operations 
 * at the lowest level possible. It is responsible for composing the <b>RPC</b> operations and send them over the wire to the data store.
 * 
 *    
 *    <p> 
 *    <b>General Contract:</b> 
 *    <ul>
 *    <li>This proxy handles  {@code byte[]} objects as key and data input/output. Serialization/Deserialization of user data does not concern this interface.</li> 
 *    <li>The proxy should encapsulate and hide all the implementations details of the <b>RPC</b> protocol in use. This does not concern the user of the proxy implementation.</li> 
 *    </ul>
 *    <p>
 *    This object offers a list of CRUD based operations over the data store. The model for the data store is a key value one. This means that all the operations over the data store 
 *    will be based on keys to access values. We choose to use {@code byte[]} as keys and also values. This choice seems to be more robust as to use simple String types for keys, given 
 *    more existent code (for example based on MAP implementations) can be easily modified to use this interface. 
 *    
 *    TODO - Explain why we do not implement a MAPinterface.  
 *    TODO - Why are there no exceptions? How to deal with error (communication system failures, etc.,) ?   We throw RunTimeExceptions for communication errors.
 *    TODO - how to key finding works (equals and hashcode).
 *    TODO - introduce mapping expression 
 *         <p>
	 * If a table with the given name already exists this method returns <code>false</code> 
	 * without modify the existent table. There is no way to distinguish between true failure (for example a network error) and an existent table. 
	 * The method {@link containsTable } covers this problem.
	 * TODO it is up to the implementation to allow or dissalow null values... Changing somewhat the documented specificaton in this interface. 
	 * 
 *    
 *   
 * @author fabiim
 *
 */

public interface TableDataStoreProxy{
	/**
	 * Empties all the data store data. 
	 * 
	 * After a call to this method the data store will be empty with absolutely no data (no tables and respective contents). 
	 */
	public  void clear();
	
	/**
	 * Creates a new table with the given name.
	 * 
	 * @param tableName the name of the table to be created
	 * @return <code>true</code> if table is successfully created;   <br/>
	 *         <code>false</code> otherwise.
	 */
	public boolean createTable(String tableName);
	
	/**
	 * Creates a new size-limited table.  
	 * <p>
	 * Creates a new table that is limited in the number of entries that it can contain. When <code>maxSize</code> is reached, 
	 * the addition of a new element to the element will result in deleting an existing one. 
	 * The  eviction policy followed is left as a decision to the implementation
	 * of this interface. 
	 * <p>
	 * If a table with the given name already exists this method returns <code>false</code> 
	 * without modify the existent table. There is no way to distinguish between true failure (for example a network error) and an existent table. 
	 * The method {@link containsTable } covers this problem. 
	 * 
	 * @param tableName the name of the table to be created
	 * @param maxSize the maximum of entries allowed in this table  
	 * @return <code>true</code> if table is successfully created;  <br/>
	 *         <code>false</code> otherwise.
	 */
	public boolean createTable(String tableName, long maxSize);
	
	/**
	 * Removes an existent table. 
	 * Removes an existent table from the data store clearing all its existent content. 
	 * To be clear, a call to method {@link #containsTable(String)} that follows a call to this method with the same tableName as argument will result in <code>false</code>. 
	 * @param tableName the name of the table to remove
	 * @return <code>true</code> if table is successfully removed;  <br/>
	 *         <code>false</code> otherwise.
	 */
	public boolean removeTable(String tableName);

	/** 
	 * Returns <code>true</code> if this data store contains a table with the specified name.
	 * @param tableName the name of the table. 
	 * @return <code>true</code> if table exists;  <br/>
	 *         <code>false</code> otherwise. 
	 */
	
	public boolean containsTable(String tableName);

	/**
	 * Empties the specified table. 
	 * <p> 
	 * After a call to this method the all the  data contained in the specified table will no longer exist.
	 * To be clear, a call to method {@link #containsKey(String, byte[])} with the same tableName specified as argument will result in <code>false</false> no matter what the specified key is.  
	 * 
	 * @param tableName the name of the table to be deleted 
	 */
	public void clear(String tableName);

	/**
	 * Returns <code>true</code> if the specified table contains no entries.
	 * 
	 * @param tableName the name of the table where the operation will be performed
	 * @return <code>true</code> if the table contains no entries; <br/>
	 *         <code>false</code> otherwise (note that it will be false when no table with the specified name exists).
	 */

	public  boolean isEmpty(String tableName);
	
	/**
	 * Returns the number of entries present in the specified table. 
	 * @param tableName the name of the table where the operation will be performed
	 * @return the number of entries present in the table if the table exists; <br/>
	 * TODO - what if it does not exists? 
	 */
	public int size(String tableName);
	
	
	/**
	 * Returns <code>true</code> if the specified table contains the specified key.
	 *  
	 * @param tableName the name of the table where the operation will be performed
	 * @param key the key of the element to search for
	 * @return <code>true</code> if the key exists;  <br/>
	 *         <code>false</code> otherwise (note that it will be false when no table with the specified name exists). 
	 */
	public boolean containsKey(String tableName, byte[] key);

	//FIXME - Should this belong here? Maybe a generic version would be cleaner, with pre-installed functions over serialized data. Maybe move up in the interface, change key to String  
	public  int getAndIncrement(String tableName, String key);

	/**
	 * @param tableName
	 * @param reference
	 * @return
	 */
	boolean createPointerTable(String tableName, String reference);
	
}


