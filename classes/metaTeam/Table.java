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
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;

public class Table implements Serializable{
	private String key;
	private ArrayList<Page> pages;
	private ArrayList<String> order;
	private String tableName;
	private int keyIndex;

	public ArrayList<String> getOrder() {
		return order;
	}

	public Table (String key, String tableName, Hashtable<String,String> colName) throws IOException {
		order = new ArrayList<String>();
		pages = new ArrayList<Page>();
		this.key = key;
		this.tableName = tableName;
		
		for(Enumeration<String> e = colName.keys(); e.hasMoreElements();) {
			String curr = e.nextElement();
			if(curr.equals(key)) {
				keyIndex = order.size();
			}
			order.add(curr);
		}
		createPage(tableName);
		updateSer();
	}

	public void insert(String tableName, Entry entry) throws IOException, ClassNotFoundException {
		FileInputStream fis = null;
		ObjectInputStream in = null;
		fis = new FileInputStream("./Data/" + tableName + ".ser");
		in = new ObjectInputStream(fis);
		Table t = (Table) in.readObject();
		ArrayList<Page> pages = t.pages;
		int pageCount = pages.size() - 1;
		fis = new FileInputStream("./Data/" + tableName + pageCount + ".ser");
		in = new ObjectInputStream(fis);
		Page p = (Page) in.readObject();
		int entryCount = getEntryCount(p.getData());
		if(entryCount == 200) {
			createPage(tableName);
			pageCount++;
			fis = new FileInputStream("./Data/" + tableName + (pageCount - 1) + ".ser");
			in = new ObjectInputStream(fis);
			p = (Page) in.readObject();
		}
		in.close();
		p.insert(entry, entryCount);
	}
	
	public void createPage(String tableName) throws IOException {
		String pageName = "" + tableName + this.pages.size();
		Page p = new Page(pageName,keyIndex);
		FileOutputStream fos = null;
		ObjectOutputStream out = null;
		try {
			fos = new FileOutputStream(new File("./Data/" + pageName + ".ser"));
			out = new ObjectOutputStream(fos);
			out.writeObject(p);
			out.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		this.pages.add(p);
		updateSer();
	}

	public void updateSer() {
		FileOutputStream fos = null;
		ObjectOutputStream out = null;
		try {
			fos = new FileOutputStream(new File("./Data/" + tableName + ".ser"));
			out = new ObjectOutputStream(fos);
			out.writeObject(this);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public ArrayList<Page> getPages() {
		return this.pages;
	}
	
	public void setPages(ArrayList<Page> pages) {
		this.pages=pages;
	}

	public int getEntryCount(Entry[] entries) {
		int count = 0;
		for (Entry entry : entries) {
			if(entry != null) {
				count++;
			}
		}
		return count;
	}
}

class Page implements Serializable{
	private String pageName;
	private Entry[] data;
	private int keyIndex;

	Page(String pageName, int keyIndex) throws IOException{
		FileReader fr = new FileReader("./config/DBApp.properties");
		BufferedReader br = new BufferedReader(fr);
		String curr = br.readLine();
		String[] h = curr.split("= ");
		int curr0 = Integer.parseInt(h[1]);
		data = new Entry[curr0];
		this.pageName = pageName;
		this.keyIndex = keyIndex;
	}

	public void setData(Entry[] data) {
		this.data=data;
	}

	public Entry[] getData() {
		return this.data;
	}
	
	public void insert(Entry entry, int entryCount) {
		data[entryCount] = entry;
		updatePage();
	}
	
	public void updatePage() {
		FileOutputStream fos = null;
		ObjectOutputStream out = null;
		try {
			fos = new FileOutputStream(new File("./Data/" + pageName + ".ser"));
			out = new ObjectOutputStream(fos);
			out.writeObject(this);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}

class Entry implements Serializable{
	private ArrayList<Object> row;
	private ArrayList<String> order;

	Entry(Hashtable<String,Object> entry,String tablename){

		row = new ArrayList<Object>();
		FileInputStream fir = null;
		ObjectInputStream in = null;

		try {
			fir = new FileInputStream(new File("./Data/" + tablename + ".ser"));
			in = new ObjectInputStream(fir);
			Table t = (Table) in.readObject();
			order = t.getOrder();
			in.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		for (String col : order) {
			Object curr = entry.get(col);
			row.add(curr);
		}
		Date d = new Date();
		row.add(d);
		
	}

	public ArrayList<Object> getRow() {
		return this.row;

	}

	public void setRow(ArrayList<Object> row) {
		this.row=row;
	}
}