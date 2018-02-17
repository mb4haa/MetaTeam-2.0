package metaTeam;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

public class Table implements Serializable{
	private String key;
	private ArrayList<Page> pages;
	private ArrayList<String> order;

	public ArrayList<String> getOrder() {
		return order;
	}

	public Table (String key, String tableName, Hashtable<String,String> colName) throws IOException {
		pages = new ArrayList<Page>();
		this.key = key;
		FileOutputStream fos = null;
		ObjectOutputStream out = null;
		try {
			fos = new FileOutputStream(new File("./Data/" + tableName + ".ser"));
			out = new ObjectOutputStream(fos);
			out.writeObject(this);

			out.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}


		for(Enumeration e = colName.keys(); e.hasMoreElements();) {
			order.add((String) e.nextElement());
		}
	}

	public void createPage(String tableName) throws IOException {
		Page p = new Page();
		FileOutputStream fos = null;
		ObjectOutputStream out = null;
		try {
			fos = new FileOutputStream(new File("./Data/" + tableName + this.pages.size() + ".ser"));
			out = new ObjectOutputStream(fos);
			out.writeObject(p);

			out.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		this.pages.add(p);
	}

	public ArrayList<Page> getPages() {
		return this.pages;

	}
	public void setPages(ArrayList<Page> pages) {
		this.pages=pages;
	}

}

class Page implements Serializable{

	private Entry[] data;

	Page() throws IOException{
		FileReader fr = new FileReader("./config/DBApp.properties");
		BufferedReader br = new BufferedReader(fr);
		String curr = br.readLine();
		String[] h = curr.split("= ");
		int curr0 = Integer.parseInt(h[1]);
		data = new Entry[curr0];
	}
	
	public void setData(Entry[] data) {
		this.data=data;
	}
	
	public Entry[] getData() {
		return this.data;
	}
}

class Entry{
	private ArrayList<Object> row;
	private ArrayList<String> order;
	
	Entry(Hashtable<String,Object> entry,String tablename){
		for(Enumeration<String> e = entry.keys();e.hasMoreElements();) {
			FileInputStream fir = null;
			ObjectInputStream in = null;

			try {
				fir = new FileInputStream(new File("./Data/" +"Student"+ ".ser"));
				in = new ObjectInputStream(fir);
				Table t = (Table)in.readObject();
				order = t.getOrder();
				in.close();
				Enumeration<String> e1 = entry.keys();
				int index =order.indexOf(e1.nextElement());
				Object toenter=entry.get(e1.nextElement());
				row.add(index, toenter);

			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

	}
	
	public ArrayList<Object> getRow() {
		return this.row;

	}
	
	public void setRow(ArrayList<Object> row) {
		this.row=row;
	}
}