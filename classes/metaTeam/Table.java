package metaTeam;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

public class Table implements Serializable{
	
	private String key;
	private int numPages;
	private ArrayList<String> order;
	private String tableName;
	private int keyIndex;

	//Constructor for the Table Class, initializes all the object attributes and creates a starter page for entries
	public Table (String key, String tableName, Hashtable<String,String> colName) throws IOException {
		this.order = new ArrayList<String>();
		this.key = key;
		this.tableName = tableName;
		this.numPages = 0;
		
		
		setKeyIndex(colName);
		createPage(tableName);
		updateSer();
	}
	
	//helper Method for Creating new Tables
	public void createPage(String tableName) throws IOException {
		String pageName = "" + tableName + this.numPages;
		Page p = new Page(pageName,keyIndex);
		
		//Writing the new Page into a file
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
		updateSer();
	}

	//helper Method for inserting new entries into the Table
	//**Unfinished**
	public void insert(String tableName, Entry entry) throws IOException, ClassNotFoundException {
		
		//Loading the last page in the table
		FileInputStream fis = null;
		ObjectInputStream in = null;
		int pageCount = this.numPages - 1;
		fis = new FileInputStream("./Data/" + tableName + pageCount + ".ser");
		in = new ObjectInputStream(fis);
		Page p = (Page) in.readObject();
		int entryCount = getEntryCount(p.getData());
		
		//checking if the last page is full, if so create a new page
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

	//helper Method for updating the serialized Table file
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

	//helper method that initializes the keyIndex attribute 
	//according to the index in the arraylist at which the key is at
	public void setKeyIndex(Hashtable<String,String> colName) {
		for(Enumeration<String> e = colName.keys(); e.hasMoreElements();) {
			String curr = e.nextElement();
			if(curr.equals(key)) {
				keyIndex = order.size();
			}
			order.add(curr);
		}
	}
	
	//helper Method that counts the number of Entries in any Page
	public int getEntryCount(Entry[] entries) {
		int count = 0;
		for (Entry entry : entries) {
			if(entry != null) {
				count++;
			}
		}
		return count;
	}
	
	
	public int getNumPages() {
		return this.numPages;
	}

	
	public ArrayList<String> getOrder() {
		return order;
	}

}
