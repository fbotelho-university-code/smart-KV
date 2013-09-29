package smartkv.server.bench;

import java.io.Serializable;
import java.util.Random;

public class TestInformation implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 9023444025964761339L;
	public final String name; 
	public final int clients; 
	public final long random; 
	
	public TestInformation(String name, int clients) {
		Random r = new Random(System.currentTimeMillis() * clients);
		this.name = name;
		this.clients = clients; 
		random = r.nextLong(); 
	}
}
