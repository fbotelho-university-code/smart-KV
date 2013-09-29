package smartkv.datastore.workloads.ReportGenerator;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import smartkv.datastore.workloads.ReportGenerator.htmlElements.SourceElement;

import com.google.common.collect.Lists;

public class Source {
	
	String description; 
	String title; 
	String sourcePath;
	List<SourceElement> elements = Lists.newArrayList();
	
	public Source(String title, String description, String indexFolder) {
		this.title = title;
		this.description = description;
		this.sourcePath = indexFolder;
	}
	
	public Source(String titleString, String description, String indexFolder,
			List<SourceElement> elements) {
		this.title = titleString; 
		this.description = description;
		this.sourcePath = indexFolder;
		this.elements = elements;
	}
	
	public String getTitle() {
		return title;
	}
	
	public void setTitle(String title){
		this.title = title;
	}
	
	public void addElement(SourceElement e){
		this.elements.add(e);
	}
	
	public void genDocument() throws IOException{
		StringBuilder out = new StringBuilder(); 
		createHeader(out);
	
		createBody(out);
		createScripts(out); 
		endBody(out);
		endDocument(out);	
		dumpToFile(out);
	}
	
	////////////////////// PRIVATE /////////////////////////////////////////////
	
	private void createScripts(StringBuilder out) {
		createHideShow(out); 
		
	}

	private void createHideShow(StringBuilder out) {
		// TODO Auto-generated method stub
		out.append("<script type=\"text/javascript\">\nfunction hideshow(which){\nif (!document.getElementById)\nreturn\nif (which.style.display==\"block\")\nwhich.style.display=\"none\"\nelse\nwhich.style.display=\"block\"\n}\n</script>"); 
	}

	private void endBody(StringBuilder out){
		out.append("</body>"); 
	}
	
	private void endDocument(StringBuilder out) {
		out.append("</html>"); 
		
	}

	
	private void dumpToFile(StringBuilder out) throws IOException {
		
		DataOutputStream outStream = new DataOutputStream(new FileOutputStream(sourcePath + "index.html"));
		outStream.write(out.toString().getBytes()); 
		outStream.flush();
		outStream.close();	
	}
	
	
	private void createBody(StringBuilder out) {
		out.append("<body>"); 
		out.append("<h1>" + title + "</h1>"); 
		out.append("<h2>Description:</h2><p>" +description +"</p>"); 
		for (SourceElement source : this.elements){
			out.append(source.render()); 
		}
	}
	
	private void createHeader(StringBuilder out) {
		String title = "\n<title>" +  getTitle() + "</title>\n" ;
		//String base = "<base href=\"" + sourcePath + "\" >";
		String base = ""; 
		String topHead = "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\" \"http://www.w3.org/TR/html4/strict.dtd\"> ";
		String pragma = "<meta http-equiv=\"Content-type\" content=\"text/html;charset=UTF-8\">";
		String csss = "<link href=\"style.css\" media=\"all\" rel=\"stylesheet\" type=\"text/css\" />";   
		String head =  topHead +  "<html> \n<head> " +  csss +pragma + title + base  + "\n</head>\n";
		out.append(head);
		}
	
}
