package smartkv.client.workloads.ReportGenerator;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import org.jfree.chart.JFreeChart;

import smartkv.client.workloads.ReportGenerator.Formats.GenerateGraphics;
import smartkv.client.workloads.ReportGenerator.Formats.PDFExporter;
import smartkv.client.workloads.ReportGenerator.htmlElements.FrequencyGenerator;
import smartkv.client.workloads.ReportGenerator.htmlElements.GroupedElement;
import smartkv.client.workloads.ReportGenerator.htmlElements.Image;
import smartkv.client.workloads.ReportGenerator.htmlElements.ListOperations;
import smartkv.client.workloads.ReportGenerator.htmlElements.SourceElement;

import com.google.common.collect.Lists;

/**
 * This class will produce a report for analyzing tests. 
 * @author fabiim
 *
 */

public class ReportGenerator {
	protected static String fileInput; 
	protected static String outputFolder; 
	protected static Source rootFile;
	private static String title;
	private static String description;
	
	public static void main(String args	[]) throws IOException{
		//TODO - check is folder . 
		switch(args.length){
			case 2:
				//singularCase(args[1]);
				//readLogAndCreateReport("./workloads/report/100.known/usedLog.0.1000","./workloads/report/", "Logs", "Hosts have static arp tables", 0, 1000);
				//readLogAndCreateReport("./workloads/report/300.known/usedLog.200.265","./workloads/report/", "Logs", "Hosts have static arp tables", 0, 1000);
				//readLogAndCreateReport("./workloads/logs","./workloads/report/", "100 No arp - Ping between known hosts", "Hosts have static arp tables", 260,10000);			
				//readLogAndCreateReport("./workloads/ewsdn.device.man","./workloads/report/", "100 No arp - Ping between known hosts", "Hosts have static arp tables", 310,10000);			
				//readLogAndCreateReport("./workloads/report/300.known/usedLog.200.265","./workloads/report/", "Logs", "", 220, 265);
				
				//readLogAndCreateReport("/Users/fabiim/dev/open/floodlight/workloadResults/lastLog","/Users/fabiim/dev/open/floodlight/workloadResults/reports/micro/APPW/WN", "TITLE", "SCRIPT",0, 10000);
				/*readLogAndCreateReport("/Users/fabiim/dev/open/floodlight/workloadResults/lastLog",
						"/Users/fabiim/dev/open/floodlight/workloadResults/reports/micro/lbw/lbw3/", 
						"LBW3 - Version values (replace with timestamps)", 
						"curl -X POST -d \'{\"id\":\"1\",\"name\":\"vip1\",\"protocol\":\"icmp\",\"address\":\"10.0.0.100\",\"port\":\"8\"}\' http://localhost:8080/quantum/v1.0/vips/\ncurl -X POST -d \'{\"id\":\"1\",\"name\":\"pool1\",\"protocol\":\"icmp\",\"vip_id\":\"1\"}\' http://localhost:8080/quantum/v1.0/pools/\ncurl -X POST -d \'{\"id\":\"1\",\"address\":\"10.0.0.2\",\"port\":\"8\",\"pool_id\":\"1\"}\' http://localhost:8080/quantum/v1.0/members/\ncurl -X POST -d \'{\"id\":\"2\",\"address\":\"10.0.0.3\",\"port\":\"8\",\"pool_id\":\"1\"}\' http://localhost:8080/quantum/v1.0/members/\n\n\nmininet@mininet-vm:~$ sudo mn --topo linear,3 --controller=remote,ip=192.168.87.1,port=6633 --mac \n*** Creating network\n*** Adding controller\n*** Adding hosts:\nh1 h2 h3 \n*** Adding switches:\ns1 s2 s3 \n*** Adding links:\n(h1, s1) (h2, s2) (h3, s3) (s1, s2) (s2, s3) \n*** Configuring hosts\nh1 h2 h3 \n*** Starting controller\n*** Starting 3 switches\ns1 s2 s3 p\n*** Starting CLI:\nmininet> pingall \n*** Ping: testing ping reachability\nh1 -> h2 h3 \nh2 -> h1 h3 \nh3 -> h1 h2 \n*** Results: 0% dropped (0/6 lost)\nCONFIGURE CONTROLLER USING THE TOP CURL COMMANDS. \nmininet> h1 ping -c1 10.0.0.100\nPING 10.0.0.100 (10.0.0.100) 56(84) bytes of data.\n64 bytes from 10.0.0.2: icmp_req=1 ttl=64 time=100 ms\n\n--- 10.0.0.100 ping statistics ---\n1 packets transmitted, 1 received, 0% packet loss, time 0ms\nrtt min/avg/max/mdev = 100.369/100.369/100.369/0.000 ms\nmininet> exit\n*** Stopping 3 hosts\nh1 h2 h3 \n*** Stopping 3 switches\ns1 ...s2 ....s3 ...\n*** Stopping 1 controllers\nc0 \n*** Done\ncompleted in 149.125 seconds\nmininet@mininet-vm:~$ \n",
						16, 10000);*/
			/*readLogAndCreateReport("/Users/fabiim/dev/open/floodlight/workloadResults/lastLog",
					"/Users/fabiim/dev/open/floodlight/workloadResults/reports/micro/lbw/lbw4/", "LBW4 - Proof of concept (cache) w/ 200MS ", "script : ./script.mininet",5, 10000);*/
				//TESTs
				//readLogAndCreateReport("/Users/fabiim/dev/open/floodlight/workloadResults/lastLog","/Users/fabiim/dev/open/floodlight/workloadResults/reports/deletable/", "LSW1 - Original with Broadcast and unicast address", "mininet@mininet-vm:~$ sudo mn --topo single,2 --controller=remote,ip=192.168.87.1,port=6633 --mac \n*** Creating network\n*** Adding controller\n*** Adding hosts:\nh1 h2 \n*** Adding switches:\ns1 \n*** Adding links:\n(h1, s1) (h2, s1) \n*** Configuring hosts\nh1 h2 \n*** Starting controller\n*** Starting 1 switches\ns1 \n*** Starting CLI:\nmininet> h1 ping -c1 h2 \nPING 10.0.0.2 (10.0.0.2) 56(84) bytes of data.\n64 bytes from 10.0.0.2: icmp_req=1 ttl=64 time=989 ms\n\n--- 10.0.0.2 ping statistics ---\n1 packets transmitted, 1 received, 0% packet loss, time 0ms\nrtt min/avg/max/mdev = 989.296/989.296/989.296/0.000 ms\nmininet> exit\n*** Stopping 2 hosts\nh1 h2 \n*** Stopping 1 switches\ns1 ...\n*** Stopping 1 controllers\nc0 \n*** Done\ncompleted in 719.269 seconds",5, 10000);
				break; 
			default: 
				System.out.println("Unknown command... See command line arguments"); 
		}
		System.out.println("end");
	}
	
	public static int BEGIN; 
	public static int END; 
	
	private static void readLogAndCreateReport(String input, String outputFolder, String title, String dsc, int i, int j) throws IOException{
		BEGIN = i; 
		END = j; 
		if (!outputFolder.endsWith("/")){
			outputFolder = outputFolder + "/"; 
		}
		initializeState(input, outputFolder, title, dsc);
		generateElementsFromInputFile();
		rootFile.genDocument(); 
	}
	
	
	private static void generateElementsFromInputFile() throws IOException{
		createReadWriteThroughput();

		//createSize(); 

		WorkLoadResults rs = new WorkLoadResults(fileInput);
		
		Stats[] st= {
				StatsCreator.readRequestSize(rs),
				StatsCreator.readResponseSize(rs),
				StatsCreator.requestSize(rs),
				StatsCreator.responseSize(rs),
				StatsCreator.writeRequestSize(rs),
				StatsCreator.writeResponseSize(rs),
		};
		
		int i = 0; 
		for (Stats s : st){
			i++; 
			GroupedElement gp = new GroupedElement(s.getTitle(), s.getDescription());
			gp.addElement(new FrequencyGenerator("Frequency Table", "X -> Tamanho em bytes da mensagem", s.freq, true));
			addFrequencyGraphic(i, s, gp);
			
		/*	List<Stats> perMethod = StatsCreator.getPerMethodStats(s);
			gp.addElement(new FrequencyGenerator("Frequency of Methods used", "", StatsCreator.getMethodCallStat(s),false));
			for (Stats pS : perMethod){
				gp.addElement(new FrequencyGenerator("Frequency Table - " + pS.getTitle() , pS.getDescription(), pS.freq, false));
			}
			for (Stats eventStats : StatsCreator.perEventStats(s)){
				gp.addElement(new FrequencyGenerator("Frequency Table  for Event Type: " + eventStats.getTitle(), eventStats.getDescription() + " - X -> size of message" , eventStats.freq, true));
			}*/
			rootFile.addElement(gp);
		}

		rootFile.addElement(new ListOperations("Operations", "The log of the operations requested to the database", new WorkLoadResults(fileInput)));
	}
	
	/**
	 * @param i
	 * @param s
	 * @param gp
	 * @throws IOException
	 */
	private static void addFrequencyGraphic(int i, Stats s, GroupedElement gp)
			throws IOException {
		String imgPath = outputFolder +   i + "freq.png";
		String imgPathPdf = outputFolder +   i + "freq.pdf";
		JFreeChart chart =  GenerateGraphics.getChartFromFreq(s.freq, s.cdfTitle , s.x, s.y);
		GenerateGraphics.saveChart( imgPath , chart, 500, 250);
		PDFExporter.writeChartToPDF(chart, 250, 100, imgPathPdf); 		
		gp.addElement(new Image("Frequency Graphic", "...", i + "freq.png"));
	}

	/**
	 * @throws IOException
	 */
	private static void createReadWriteThroughput() throws IOException {
		JFreeChart chart = GenerateGraphics.createReadWriteThroughPut(new WorkLoadResults(fileInput), false);
		String out = outputFolder + "read.write.throughput.png"; 		
		String outPdf = outputFolder + "read.write.throughput.pdf";
		GenerateGraphics.saveChart( out, chart, 750, 500);
		SourceElement img = new Image("Read/Write Requests per Second ", "The number of write and read requests that have been sent to the datastore on every second.","read.write.throughput.png"); 
		
		chart = GenerateGraphics.createReadWriteThroughPut(new WorkLoadResults(fileInput), true);
		out = outputFolder + "read.write.throughput.requests.png";
		outPdf = outputFolder + "read.write.throughput.requests.pdf";
		GenerateGraphics.saveChart( out, chart, 750, 500);
		SourceElement img2 = new Image("Events over time", "Events (green - link addition ; yellow - new topology ; blue - link removal) -happening over time ","read.write.throughput.requests.png");
		
		rootFile.addElement(new GroupedElement ("ReadWrite Throughput", "Requests sent to the databse", Lists.newArrayList(img, img2))); 
	}
	
	private static void createSize() throws IOException{
		JFreeChart chart = GenerateGraphics.createSize(new WorkLoadResults(fileInput), false);
		String out = outputFolder + "read.write.size.png"; 		
		GenerateGraphics.saveChart( out, chart, 750, 500);
		SourceElement img = new Image("Read/Write total size p/Second", "The total number of bytes received/requested to the datastore on each second","read.write.size.png"); 
	
		chart = GenerateGraphics.createSizeM(new WorkLoadResults(fileInput), false);
		out = outputFolder + "read.write.size2.png";
		
		String outPdf = outputFolder + "read.write.size2.pdf"; 		
		GenerateGraphics.saveChart( out, chart, 750, 500);
		SourceElement img2 = new Image("Read/Write mean size  per Second ", "The mean number of bytes requested/received to the datastore on each second","read.write.size2.png"); 
		
		PDFExporter.writeChartToPDF(chart, 500, 350, outPdf); 		
		rootFile.addElement(new GroupedElement("Read/Write response/request over time", "", Lists.newArrayList(img,img2)));
	}
	
	/**
	 * @param input
	 * @param outputFolder
	 * @param dsc 
	 * @param title2 
	 * @throws IOException
	 */
	private static void initializeState(String input, String outputFolder, String title2, String dsc)
			throws IOException {
		fileInput = input;
		
		File in = new File (input); 
		
		if (!in.exists()){
			System.err.println("File : " + fileInput + " does not exist" ); 
			System.exit(-1);
		}
		ReportGenerator.outputFolder = outputFolder;
		File f = new File(outputFolder);
		if (!f.exists()){
			f.mkdirs();
		}
		if (!f.isDirectory()){
			throw new IOException("not a directory: " + outputFolder); 
		}
		copyFile(new File(input), new File(outputFolder + "usedLog." + BEGIN + "." + END ));
		if (!outputFolder.endsWith("workloads/report/"))
			copyFile(new File("/Users/fabiim/dev/open/floodlight/workloadResults/reports/style.css"), new File(outputFolder + "style.css")); 
		dsc = dsc.replaceAll("\n", "<br/>"); 
		rootFile = new Source(title2, dsc, outputFolder) ;
	}
	
	public static void copyFile(File sourceFile, File destFile) throws IOException {
		if(!destFile.exists()) {
	        destFile.createNewFile();
	    }
	    FileChannel source = null;
	    FileChannel destination = null;

	    try {
	        source = new FileInputStream(sourceFile).getChannel();
	        destination = new FileOutputStream(destFile).getChannel();
	        destination.transferFrom(source, 0, source.size());
	    }
	    finally {
	        if(source != null) {
	            source.close();
	        }
	        if(destination != null) {
	            destination.close();
	        }
	    }
	}
}
