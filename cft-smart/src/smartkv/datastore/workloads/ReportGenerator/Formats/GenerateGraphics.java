package smartkv.datastore.workloads.ReportGenerator.Formats;

import java.awt.Color;
import java.awt.GradientPaint;
import java.io.File;
import java.io.IOException;
import java.util.List;

import mapserver.RequestType.SuperType;

import org.apache.commons.math3.stat.Frequency;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.labels.StandardCategoryItemLabelGenerator;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.ValueMarker;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.category.StackedBarRenderer;
import org.jfree.chart.title.LegendTitle;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.RectangleEdge;

import smartkv.datastore.workloads.ActivityEvent;
import smartkv.datastore.workloads.RequestLogEntry;
import smartkv.datastore.workloads.ReportGenerator.Stats;
import smartkv.datastore.workloads.ReportGenerator.StatsCreator;
import smartkv.datastore.workloads.ReportGenerator.WorkLoadResults;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

public class GenerateGraphics {

public static JFreeChart createReadWriteThroughPut(WorkLoadResults wr, boolean writeEventLabels){
	
		List<List<RequestLogEntry>> writesPerSecond = wr.getRequestsPerIntervalWithEmptyIntervals(1000, SuperType.WRITE);
		List<List<RequestLogEntry>> readsPerSecond = wr.getRequestsPerIntervalWithEmptyIntervals(1000, SuperType.READ);
		
		int writes =0; 
		final XYSeries writesSeries = new XYSeries("Writes");
			for (int i =0 ; i < writesPerSecond.size() ; i++){
				List<RequestLogEntry> list = writesPerSecond.get(i);
				writesSeries.add(i, list.size());
				writes += list.size(); 
			}
			System.out.println("writes: - Total:" + wr.getWrites().size()  + " ; PerSecond: "+ writes);
		int reads = 0;
		final XYSeries readSeries = new XYSeries("Read Throughput");
		{
			for (int i =0 ; i < readsPerSecond.size() ; i++){
				List<RequestLogEntry> list = readsPerSecond.get(i); 
				readSeries.add(i, list.size());
				reads += list.size(); 
			}
			System.out.println("reads - Total: " + wr.getReads().size() + " ; PerSecond: " + reads);
		}
		
		
		final XYSeriesCollection data = new XYSeriesCollection(writesSeries);
		data.addSeries(readSeries);
		
		
        final JFreeChart chart = ChartFactory.createXYLineChart(
            "",
            "Time (seconds)",
            "#Requests", 
            data,
            PlotOrientation.VERTICAL,
            true,
            true,
            false
        );
        
        long timeStartReference = wr.getTimeZero();
		XYPlot plot = (XYPlot) chart.getPlot();
		
        //Set events :
    		for (ActivityEvent e : wr.getActivityLog()){
      			switch(e.getType()){
/*      			case LINK_ADDED:
      				if (writeEventLabels)
      					addMarker(timeStartReference, e, plot, Color.GREEN, null,true);
      				break;
      			case LINK_REMOVED:
      				if (writeEventLabels)
      					addMarker(timeStartReference, e, plot, Color.CYAN, null,true);
      				break;
*/
      			case NEW_TOPOLOGY_INSTANCE:
      				if (writeEventLabels)
      					addMarker(timeStartReference, e, plot, Color.GREEN, null, true);
      				break;
      			case START_NETWORK:
      				//addIntervarlMarker(timeStartReference,e.getTimeStart(), e.getTimeEnd(), plot, Color.DARK_GRAY, (float) 0.1, "Network Init");
      				addMarker(timeStartReference, e, plot, Color.black, "IB", true); 
      				addMarker(timeStartReference, e, plot, Color.black, "IE", false); 
      				break;
      			case STOP_NETWORK:
      				//addIntervarlMarker(timeStartReference, e.getTimeStart(), e.getTimeEnd() + 100000,  plot, Color.DARK_GRAY, (float) 0.9, "Network stopped"); 
      				addMarker(timeStartReference, e, plot, Color.BLACK, "S",true); 
      				break;
				case REMOVING_LINKS:
					addMarker(timeStartReference, e, plot, Color.pink, "RB", true); 
					addMarker(timeStartReference, e, plot, Color.pink, "RE", false);
				case SWITCH_TURN_OFF: 
					addMarker(timeStartReference, e, plot, Color.MAGENTA, "TB", true);
					addMarker(timeStartReference, e, plot, Color.MAGENTA, "TE", false);
					//addIntervarlMarker(timeStartReference, e.getTimeStart(), e.getTimeEnd(), plot, Color.DARK_GRAY, (float) 0.5, "Start Removing Links"); 
					break;
				default: 
						
      			}
      		}

        plot.setBackgroundPaint(Color.WHITE); 
    		return chart; 
     
	}

public static JFreeChart createSize(WorkLoadResults wr, boolean writeEventLabels){
	
	List<List<RequestLogEntry>> writesPerSecond = wr.getRequestsPerIntervalWithEmptyIntervals(1000, SuperType.WRITE);
	List<List<RequestLogEntry>> readsPerSecond = wr.getRequestsPerIntervalWithEmptyIntervals(1000, SuperType.READ);
	
	int writes =0; 
	final XYSeries writesSeries = new XYSeries("Write Request Total size");
		for (int i =0 ; i < writesPerSecond.size() ; i++){
			List<RequestLogEntry> list = writesPerSecond.get(i);
			Stats s = StatsCreator.writeRequestSize( new WorkLoadResults(list, wr.getActivityLog()));
			int size=0;
			for (RequestLogEntry l : list){
				size += l.getSizeOfRequest(); 
				
			}
			writesSeries.add(i,size);
		}
	int reads = 0;
	final XYSeries readSeries = new XYSeries("Read Response Total size");
	{
		for (int i =0 ; i < readsPerSecond.size() ; i++){
			List<RequestLogEntry> list = readsPerSecond.get(i);
			Stats s = StatsCreator.readResponseSize( new WorkLoadResults(list, wr.getActivityLog()));
			int size = 0;
			for (RequestLogEntry l : list){
				size += l.getSizeOfResponse();
				
			}
			readSeries.add(i,size); 	
			
		}
	}
	
	
	final XYSeriesCollection data = new XYSeriesCollection(writesSeries);
	data.addSeries(readSeries);
	
	
    final JFreeChart chart = ChartFactory.createXYLineChart(
        "",
        "Time (seconds)",
        "#size in bytes", 
        data,
        PlotOrientation.VERTICAL,
        true,
        true,
        false
    );
    long timeStartReference = wr.getTimeZero();
	XYPlot plot = (XYPlot) chart.getPlot();
		
    
    //Set events :
		for (ActivityEvent e : wr.getActivityLog()){
  			switch(e.getType()){
  			case LINK_ADDED:
  				if (writeEventLabels)
  					addMarker(timeStartReference, e, plot, Color.GREEN, null,true);
  				break;
  			case LINK_REMOVED:
  				if (writeEventLabels)
  					addMarker(timeStartReference, e, plot, Color.CYAN, null,true);
  				break;
  			case NEW_TOPOLOGY_INSTANCE:
  				if (writeEventLabels)
  					addMarker(timeStartReference, e, plot, Color.YELLOW,null, true);
  				break;
  			case START_NETWORK:
  				//addIntervarlMarker(timeStartReference,e.getTimeStart(), e.getTimeEnd(), plot, Color.DARK_GRAY, (float) 0.1, "Network Init");
  				addMarker(timeStartReference, e, plot, Color.black, "IB", true); 
  				addMarker(timeStartReference, e, plot, Color.black, "IE", false); 
  				break;
  			case STOP_NETWORK:
  				//addIntervarlMarker(timeStartReference, e.getTimeStart(), e.getTimeEnd() + 100000,  plot, Color.DARK_GRAY, (float) 0.9, "Network stopped"); 
  				addMarker(timeStartReference, e, plot, Color.BLACK, "S",true); 
  				break;
			case REMOVING_LINKS:
				addMarker(timeStartReference, e, plot, Color.pink, "RB", true); 
				addMarker(timeStartReference, e, plot, Color.pink, "RE", false);
			case SWITCH_TURN_OFF: 
				addMarker(timeStartReference, e, plot, Color.MAGENTA, "TB", true);
				addMarker(timeStartReference, e, plot, Color.MAGENTA, "TE", false);
				//addIntervarlMarker(timeStartReference, e.getTimeStart(), e.getTimeEnd(), plot, Color.DARK_GRAY, (float) 0.5, "Start Removing Links"); 
				break;
  			}
  		}

    plot.setBackgroundPaint(Color.WHITE); 
		return chart; 
 
}
public static JFreeChart createSizeM(WorkLoadResults wr, boolean writeEventLabels){
	
	List<List<RequestLogEntry>> writesPerSecond = wr.getRequestsPerIntervalWithEmptyIntervals(1000, SuperType.WRITE);
	List<List<RequestLogEntry>> readsPerSecond = wr.getRequestsPerIntervalWithEmptyIntervals(1000, SuperType.READ);
	
	int writes =0; 
	final XYSeries writesSeries = new XYSeries("Write Request mean size");
		for (int i =0 ; i < writesPerSecond.size() ; i++){
			List<RequestLogEntry> list = writesPerSecond.get(i);
			Stats s = StatsCreator.writeRequestSize( new WorkLoadResults(list, wr.getActivityLog()));
			int size=0;
			
			writesSeries.add(i,s.dstats.getMean()/1024);
		}
	int reads = 0;
	final XYSeries readSeries = new XYSeries("Read Response Mean size");
	{
		for (int i =0 ; i < readsPerSecond.size() ; i++){
			List<RequestLogEntry> list = readsPerSecond.get(i);
			Stats s = StatsCreator.readResponseSize( new WorkLoadResults(list, wr.getActivityLog()));
			int size = 0;
			readSeries.add(i,s.dstats.getMean() / 1024); 	
			
		}
	}
	
	final XYSeriesCollection data = new XYSeriesCollection(writesSeries);
	data.addSeries(readSeries);
	
	
    final JFreeChart chart = ChartFactory.createXYLineChart(
        "",
        "Time (s)",
        "Mean Message Size (kB)", 
        data,
        PlotOrientation.VERTICAL,
        true,
        true,
        false
    );
    long timeStartReference = wr.getTimeZero();
	XYPlot plot = (XYPlot) chart.getPlot();
	LegendTitle t = chart.getLegend();
	t.setPosition(RectangleEdge.TOP); 
	
	//plot.getRangeAxis().setRange(0, 80); 
	//plot.getDomainAxis().setRange(600, 1200); 
    //Set events :
		for (ActivityEvent e : wr.getActivityLog()){
  			switch(e.getType()){
/*  			case LINK_ADDED:
  				if (writeEventLabels)
  					addMarker(timeStartReference, e, plot, Color.GREEN, null,true);
  				break;
  			case LINK_REMOVED:
  				if (writeEventLabels)
  					addMarker(timeStartReference, e, plot, Color.CYAN, null,true);
  				break;
*/
  			case NEW_TOPOLOGY_INSTANCE:
  				if (writeEventLabels)
  					addMarker(timeStartReference, e, plot, Color.GREEN,null, true);
  				break;
  			case START_NETWORK:
  				//addIntervarlMarker(timeStartReference,e.getTimeStart(), e.getTimeEnd(), plot, Color.DARK_GRAY, (float) 0.1, "Network Init");
  			//	addMarker(timeStartReference, e, plot, Color.black, "IB", true); 
  			//	addMarker(timeStartReference, e, plot, Color.black, "IE", false); 
  				break;
  			case STOP_NETWORK:
  				//addIntervarlMarker(timeStartReference, e.getTimeStart(), e.getTimeEnd() + 100000,  plot, Color.DARK_GRAY, (float) 0.9, "Network stopped"); 
  				addMarker(timeStartReference, e, plot, Color.BLACK, "S",true); 
  				break;
			case REMOVING_LINKS:
				//addMarker(timeStartReference, e, plot, Color.pink, "RB", true); 
				//addMarker(timeStartReference, e, plot, Color.pink, "RE", false);
				break; 
			case SWITCH_TURN_OFF: 
				//addMarker(timeStartReference, e, plot, Color.MAGENTA, "TB", true);
				//addMarker(timeStartReference, e, plot, Color.MAGENTA, "TE", false);
				//addIntervarlMarker(timeStartReference, e.getTimeStart(), e.getTimeEnd(), plot, Color.DARK_GRAY, (float) 0.5, "Start Removing Links"); 
				break;
			default: 
					
  			}
  		}

    plot.setBackgroundPaint(Color.WHITE); 
		return chart; 
 
}

	public static void saveChart(String path, JFreeChart chart, int width, int weight) throws IOException{
		File f = new File(path);
		ChartUtilities.saveChartAsPNG(f, chart, width, weight);
	}
	
	
	/**
	 * @param timeStartReference
	 * @param e
	 * @return
	 */
	private static void addMarker(long timeStartReference,
			ActivityEvent e, XYPlot plot , Color c, String label, boolean beginning) {
		ValueMarker marker = new ValueMarker( beginning  ? (e.timeStart - timeStartReference) /1000.0: (e.timeEnd - timeStartReference) / 1000.0);
		marker.setAlpha((float) 1); 
		marker.setPaint(c);
		/*if (label != null) {
			marker.setLabel(label);
		}*/
		plot.addDomainMarker(marker); 
	}
	
/*	public static class Solve extends  StandardCategoryItemLabelGenerator {
		@Override
		public String generateLabel(CategoryDataset dataset, int row, int column){
			DefaultCategoryDataset s = new DefaultCategoryDataset();
			s.addValue(dataset.getValue(row, column).doubleValue() * 100.0, "P[x <= X] ", "");
			return super.generateLabel(s, row, column);
			//new DefaultCategoryDataset(dataset.getValue(row, column).doubleValue() * 100, dataset.getRowKey(row), dataset.getColumnKey(column)); 
		}

	}*/
	public static JFreeChart getChartFromFreq(Frequency freq, String Title, String x, String y){
		DefaultCategoryDataset dataset = getDataSetFromFrequency(freq);
		
		// create the chart...
		final JFreeChart chart = ChartFactory.createBarChart(
				Title,         // chart title
				x,               // domain axis label
				y,                  // range axis label
				dataset,                  // data
				PlotOrientation.VERTICAL, // orientation
				true,                     // include legend
				true,                     // tooltips?
				false                     // URLs?
				);
		
		
		// NOW DO SOME OPTIONAL CUSTOMISATION OF THE CHART...
		// set the background color for the chart...
		chart.setBackgroundPaint(Color.white);
		
		// get a reference to the plot for further customisation...
		final CategoryPlot plot = chart.getCategoryPlot();
		plot.setBackgroundPaint(Color.lightGray);
		plot.setDomainGridlinePaint(Color.white);
		plot.setRangeGridlinePaint(Color.white);
		
		StackedBarRenderer renderer = new StackedBarRenderer(false);
/*		renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
		renderer.setBaseItemLabelsVisible(true);
		chart.getCategoryPlot().setRenderer(renderer);
	*/	
		// set the range axis to display integers only...
		//final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
		//rangeAxis.setStandaepordTickUnits(NumberAxis.createIntegerTickUnits());

		// disable bar outlines...
		chart.getCategoryPlot().setRenderer(renderer);
		renderer.setDrawBarOutline(false);

		// set up gradient paints for series...
		
		/*final GradientPaint gp1 = new GradientPaint(
				0.0f, 0.0f, Color.green, 
				0.0f, 0.0f, Color.lightGray
				);
		final GradientPaint gp2 = new GradientPaint(
				0.0f, 0.0f, Color.red, 
				0.0f, 0.0f, Color.lightGray
				);*/
		renderer.setSeriesPaint(0, Color.BLUE);
		//renderer.setSeriesPaint(1, gp1);
		//renderer.setSeriesPaint(2, gp2);

		final CategoryAxis domainAxis = plot.getDomainAxis();
		domainAxis.setCategoryLabelPositions(
				CategoryLabelPositions.createUpRotationLabelPositions(Math.PI / 6.0)
				);
		
		
		// OPTIONAL CUSTOMISATION COMPLETED.
		return chart;
	}
	
	/**
	 * @param freq
	 */
	private static DefaultCategoryDataset getDataSetFromFrequency(Frequency freq) {
		
		DefaultCategoryDataset dataset = new DefaultCategoryDataset();
		PeekingIterator<Comparable<?>> it = Iterators.peekingIterator(freq.valuesIterator());
		double count=0, pct=0, cumPct =0; 
		while (it.hasNext()){
			Comparable<?> value = it.next();
			count = freq.getCount(value);
			pct = freq.getPct(value);
			cumPct = freq.getCumPct(value); 
			double max_value = 0.05 - pct; 
			Comparable<?> lastValueInInterval =null; //Represents that we have now a set since the percentage of the value is small (less than max_value) 
			while (it.hasNext() && (max_value - freq.getPct(it.peek()) ) >= 0 ){
				Comparable<?> nextValue = it.next();
				max_value -= freq.getPct(nextValue);
				count += freq.getCount(nextValue);
				pct += freq.getPct(nextValue);
				cumPct += freq.getPct(nextValue);  
				lastValueInInterval = nextValue; 
			}
			
			String category = lastValueInInterval == null ?  value.toString() : value.toString() + "-" + lastValueInInterval.toString() ;
			
			dataset.addValue(cumPct, "P[x <= X] ", category);
			//dataset.addValue(pct, "P[X] ", category);

		}
		return dataset;
	}
}

