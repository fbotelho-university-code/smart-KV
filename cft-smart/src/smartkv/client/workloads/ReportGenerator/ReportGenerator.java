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
				readLogAndCreateReport("./workloads/logs.objectsctrlc","./workloads/report/", "Test", "",0, 1000);
				break; 
			default: 
				System.out.println("Unknown command... See command line arguments"); 
		}
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
		System.out.println("Created Read/Write Throughput"); 
		//createSize(); 
		System.out.println("Created Sizes"); 
		WorkLoadResults rs = new WorkLoadResults(fileInput);
		
		Stats[] st= {
				//StatsCreator.readRequestSize(rs),
				StatsCreator.readResponseSize(rs),
				//StatsCreator.requestSize(rs),
				//StatsCreator.responseSize(rs),
				StatsCreator.writeRequestSize(rs),
				//StatsCreator.writeResponseSize(rs),
		};
		
		int i = 0; 
		for (Stats s : st){
			System.out.println("One more s "); 
			i++; 
			GroupedElement gp = new GroupedElement(s.getTitle(), s.getDescription());
			gp.addElement(new FrequencyGenerator("Frequency Table", "X -> Tamanho em bytes da mensagem", s.freq, true));
			//addFrequencyGraphic(i, s, gp);
			
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
			copyFile(new File("./workloads/report/style.css"), new File(outputFolder + "style.css")); 
		
		rootFile = new Source(title2, description, outputFolder) ;
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
