package smartkv.datastore.workloads.ReportGenerator.htmlElements;


public abstract class SourceElement {
	private static int ID=0;
	public static int getID() {
		return ID;
	} 
	
	private String description;
	private int  id; 
	private String rootPath; 
	private String title; 
	
	protected StringBuilder out = new StringBuilder();

	public SourceElement(String title, String description) {
		super();
		id = SourceElement.ID++; 
		this.title = title;
		this.description = description;
	}

	public String getDescription() {
		return description;
	}

	public int getId() {
		return id;
	}

	public StringBuilder getOut() {
		return out;
	}

	public String getRootPath() {
		return rootPath;
	}
	
	public String getTitle() {
		return title;
	}
	
	public String render(){
		startRender();
		renderBody(); 
		endRender();
		
		return out.toString(); 
	}
	
	public String getHeader(){
		StringBuilder backUp =  new StringBuilder(out.toString());
		out = new StringBuilder(); 
		startRender(); 
		endRender();
		String toReturn = out.toString(); 
		out = backUp; 
		return toReturn; 
	}
	
	public String getBody(){
		StringBuilder backUp =  new StringBuilder(out.toString());
		out = new StringBuilder(); 
		renderBody();
		String toReturn = out.toString(); 
		out = backUp; 
		return toReturn; 
	}
	
	public  String toString(){
		return out.toString(); 
	}
	
	public abstract void renderBody();
	
	public void setDescription(String description) {
		this.description = description;
	}

	public void setId(int id) {
		this.id = id;
	}
	
	public void setOut(StringBuilder out) {
		this.out = out;
	}
		
	public void setRootPath(String rootPath) {
		this.rootPath = rootPath;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	protected void endDiv(){
		out.append("</div>");
	}
	
	private void renderDescription() {
		startDiv(id + "description", "description_top_element");
		out.append("<p>" + description + "</p>\n");
		endDiv(); 
	}

	private void renderTitle() {
		out.append("<h2>" + title + "</h2>\n");
	}

	protected void startDiv(String divId, String divClass){
		out.append("<div " + "id=\"" + divClass + "\">\n");
	}
	
	protected void endRender(){
		endDiv(); 
		out.append("<hr>\n"); 
	}
	
	protected void startRender(){
		startDiv( id + "", "topelement");
		renderTitle(); 
		renderDescription(); 
	}
}
