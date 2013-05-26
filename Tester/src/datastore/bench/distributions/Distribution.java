package datastore.bench.distributions;

import java.util.Random;

public class Distribution {
	public static Distribution HostsDeviceManager1000Writes = new Distribution(new double[][]{
			{28.38, 488, 488},  
			{71.62, 1800, 1800}, 
			{85.81, 3500, 3500}, 
			{100.00, 466566,466566}
		});  
	public static Distribution HostsDeviceManager1000Reads = new Distribution(new double[][]{
			{39.25, 82.0, 82.0},
			{46.42, 231.0, 231.0}, 
			{66.61, 1680, 1680}, 
			{68.66, 1692, 1758}, 
			{85.66, 1770, 1770}, 
			{100, 466516,466516.0 }
		});
	
	
	
	public final double[][] distribution;  
	public final byte[][] msgs; 
	private final Random generator = new Random(); 
	public  Distribution(double[][] distr){
		this.distribution = distr;
		this.msgs = new byte[this.distribution.length][]; 
		for (int i =0 ; i < this.distribution.length; i++){
			this.msgs[i] = new byte[(int) this.distribution[i][2]]; 
		}
	}
	
	public byte[] getRandomMsg(){
		double val = generator.nextDouble() * 100.00;		
		for (int i=0; i < distribution.length; i++){
			if (val < distribution[i][0]){
				return new byte[(int)distribution[i][2]]; 
			}
		}
		return new byte[(int) distribution[distribution.length-1][2]]; 
	}
}
