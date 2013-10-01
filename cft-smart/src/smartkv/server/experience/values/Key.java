/**
 * 
 */
package smartkv.server.experience.values;

import java.io.Serializable;

/**
 * @author fabiim
 *
 */
public interface Key extends Comparable<Key>, Serializable{

	public abstract byte[] asByteArray();

	public abstract boolean equals(Object obj);

}