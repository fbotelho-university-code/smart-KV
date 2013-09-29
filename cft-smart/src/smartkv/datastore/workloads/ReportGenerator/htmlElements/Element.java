package smartkv.datastore.workloads.ReportGenerator.htmlElements;

public interface Element {

	public abstract String getDescription();

	public abstract int getId();

	public abstract StringBuilder getOut();

	public abstract String getRootPath();

	public abstract String getTitle();

	public abstract String toString();

	public abstract String  render(); 
	public abstract void renderBody();

	public abstract void setDescription(String description);

	public abstract void setId(int id);

	public abstract void setOut(StringBuilder out);

	public abstract void setRootPath(String rootPath);

	public abstract void setTitle(String title);

}