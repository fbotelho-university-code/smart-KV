package smartkv.client.workloads.ReportGenerator.htmlElements;


import java.util.List;

public abstract class TableGenerator extends SourceElement{

	public TableGenerator(String title, String description) {
		super(title, description);
		// TODO Auto-generated constructor stub
	}

	List<double[]> tableContents; 
	List<String> lines; 
	List<String> columns; 

	@Override
	public void  renderBody() {
		String begin = "<table border=1>"; 
		String end = "</table>";
		
		out.append(begin);
		genValues(); 
		out.append(end);
	}
	
	private String genValues() {
			out.append("<tr>");
		for (String col : columns){
			out.append("<th> " + col + "</th>");
		}
		
		out.append("</tr>");
		
		for (int i =0 ; i <lines.size() ; i ++){
			out.append("<tr>\n");
			out.append("<th>" + lines.get(i) + "</th>");
			renderValueLine(i);
			out.append("</tr>\n"); 
		}
		return out.toString(); 
	}

	protected void renderValueLine(int line){
		double[] values = tableContents.get(line);
		for (int i = 0 ; i < values.length ; i++){
			startCellValue();
			renderValue(values[i]);
			endCellValue(); 
		}
	}
	
	protected void endCellValue() {
		out.append("</td>");
	}
	
	protected void startCellValue() {
		out.append("<td>");
	}


	protected void renderValue(double v){
		out.append(v + ""); 
	}
	
}
