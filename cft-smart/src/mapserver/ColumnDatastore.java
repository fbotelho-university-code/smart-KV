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
}
