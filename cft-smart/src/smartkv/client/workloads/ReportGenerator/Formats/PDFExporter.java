package smartkv.client.workloads.ReportGenerator.Formats;

import java.awt.Graphics2D;
import java.awt.geom.Rectangle2D;
import java.io.FileOutputStream;

import org.jfree.chart.JFreeChart;

import com.lowagie.text.Document;
import com.lowagie.text.pdf.DefaultFontMapper;
import com.lowagie.text.pdf.PdfContentByte;
import com.lowagie.text.pdf.PdfTemplate;
import com.lowagie.text.pdf.PdfWriter;

public class PDFExporter {

	public static void writeChartToPDF(JFreeChart chart, int width, int height, String fileName) {
	    PdfWriter writer = null;
	 
	    Document document = new Document();
	 
	    try {
	        writer = PdfWriter.getInstance(document, new FileOutputStream(
	                fileName));
	        document.open();
	        PdfContentByte contentByte = writer.getDirectContent();
	        PdfTemplate template = contentByte.createTemplate(width, height);
	        Graphics2D graphics2d = template.createGraphics(width, height,
	                new DefaultFontMapper());
	        Rectangle2D rectangle2d = new Rectangle2D.Double(0, 0, width,
	                height);
	        
	        chart.draw(graphics2d, rectangle2d);
	         
	        graphics2d.dispose();
	        contentByte.addTemplate(template, 0, 0);
	 
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	    document.close();
	}

}
