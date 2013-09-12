/**
 * 
 */
package mapserver;

import java.io.DataInputStream;



/**
 * @author fabiim
 *
 */
public interface Datastore {
	 byte[] get_and_increment(DataInputStream msg) throws Exception;
	 byte[] create_table(DataInputStream msg)  throws Exception;
	 byte[] create_table_max_size(DataInputStream msg)  throws Exception; 
	 byte[] remove_table(DataInputStream msg)  throws Exception; 
	 byte[] contains_table(DataInputStream msg) throws Exception; 
	 byte[] clear() throws Exception; 
	 byte[] clear_table(DataInputStream msg) throws Exception; 
	 byte[]  contains_key_in_table(DataInputStream msg) throws Exception;
	 byte[]  get_table(DataInputStream msg) throws Exception;
	 byte[]  get_value_in_table(DataInputStream msg) throws Exception; 
	 byte[]  is_datastore_empty() throws Exception;
	 byte[]  is_table_empty(DataInputStream msg) throws Exception;
	 byte[]  put_value_in_table(DataInputStream msg) throws Exception; 
	 byte[]  put_Values_in_table(DataInputStream msg) throws Exception;
	 byte[]  remove_value_from_table(DataInputStream msg) throws Exception;
	 byte[]  size_of_table(DataInputStream msg) throws Exception;
	 byte[]  atomic_replace_value_in_table(DataInputStream msg) throws Exception;
	 byte[]  atomic_remove_if_value(DataInputStream msg) throws Exception;
	 byte[]  atomic_put_if_absent(DataInputStream msg ) throws Exception;
	 byte[] values(DataInputStream msg) throws Exception; 
	/**
	 * @param dis
	 * @return
	 * @throws Exception 
	 */
	byte[] insert_value_in_table(DataInputStream dis) throws Exception;
}

