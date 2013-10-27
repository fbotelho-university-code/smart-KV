/**
 * 
 */
package smartkv.server;

import java.io.DataInputStream;
import java.io.IOException;

import smartkv.server.experience.KeyColumnValueStore;

/**
 * @author fabiim
 *
 */
public interface ColumnDatastore extends Datastore{
	public byte[] get_column(DataInputStream stream) throws Exception; 
	public byte[] put_column(DataInputStream stream) throws Exception;
	
	/**
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	byte[] get_column_by_reference(DataInputStream dis) throws IOException;
	/**
	 * @param dis
	 * @return
	 * @throws IOException 
	 */
	public byte[] get_columns(DataInputStream dis) throws IOException;
	/**
	 * @return
	 */
	public KeyColumnValueStore getDatastore();
	/**
	 * @param dis
	 * @return
	 * @throws Exception 
	 */
	public byte[] replace_column(DataInputStream dis) throws Exception;
}
