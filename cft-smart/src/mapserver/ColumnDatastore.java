/**
 * 
 */
package mapserver;

import java.io.DataInputStream;
import java.io.IOException;

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
}
