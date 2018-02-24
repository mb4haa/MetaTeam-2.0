package metaTeam;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;

@SuppressWarnings("serial")
public class Table implements Serializable{

	private String key;
	private int numPages;
	private ArrayList<String> order;
	private String tableName;
	private int keyIndex;
	private String keyType;

	//Constructor for the Table Class, initializes all the object attributes and creates a starter page for entries
	public Table (String key, String tableName, Hashtable<String,String> colName) throws IOException {
		this.order = new ArrayList<String>();
		this.key = key;
		this.tableName = tableName;
		this.numPages = 0;
		this.setKeyType(colName.get(key));

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
		numPages++;
		updateSer();
	}

	//helper Method for inserting new entries into the Table
	//**Unfinished**
	public void insert(Entry entry) throws IOException, ClassNotFoundException {
		//MIDANY-KHAIRY-NOUR-MARIAM-BOUDI(bahaa helped 30%) WROTE THIS 
		//q
		//logic kolo bta3na
		//not bahaa
		//nafs el fekra ya3ny
		//Loading the last page in the table
		FileInputStream fis = null;
		ObjectInputStream in = null;
		int pageCount = this.numPages - 1;
		fis = new FileInputStream("./Data/" + tableName + pageCount + ".ser"); //change page
		in = new ObjectInputStream(fis);
		Page p = (Page) in.readObject();
		int entryCount = getEntryCount(p.getData());

		if(entryCount == 0) {
			p.insert(entry, 0);
			fis.close();
			in.close();
			return;
		}
		for (int i = 0; i < this.numPages ; i++)
		{
			fis = new FileInputStream("./Data/" + tableName + "" + i + ".ser");
			in = new ObjectInputStream(fis);
			p = (Page) in.readObject();
			entryCount = getEntryCount(p.getData());
			Entry lastEntry = p.getData()[entryCount-1];
			int q = compareEntries(entry, lastEntry);
			System.out.println(q);
			if(q == -1){
				for (int qq = 0; qq < entryCount  ; qq++){	
					Entry entryq = p.getData()[qq];
					if(compareEntries(entry,entryq) == -1){
						p.getData()[qq] = entry;
						System.out.println(entry.getRow().get(2));
						entry = entryq;
						p.updatePage();
						System.out.println("Mariam");
					}
				}
				break;
			}
			else if (getEntryCount(p.getData()) != p.getData().length){
				System.out.println("da5al?");
				p.insert(entry, entryCount);
				fis.close();
				in.close();
				return;
			}
			else if(i == this.numPages - 1 && getEntryCount(p.getData()) == p.getData().length){
				createPage(tableName);
				int newPage = i + 1;
				fis = new FileInputStream("./Data/" + tableName + newPage + ".ser");
				in = new ObjectInputStream(fis);
				p = (Page) in.readObject();
				p.insert(entry,0);
				fis.close();
				in.close();
				return;
			}
		}
		fis.close();
		in.close();
		updateSer();
		if(! searchTable(entry)) {
			insert(entry);
		}
	}
	
	public static Entry edit(Entry newEntry, Entry oldEntry) {
		int count = 0;
		for (Object obj : newEntry.getRow()) {
			if(obj != null) {
				oldEntry.getRow().set(count, obj);
			}
			count++;
		}
		return oldEntry;
	}
	
	public void update(Hashtable<String, Object> colNameValue ) throws ClassNotFoundException, IOException {	
		Entry entry = new Entry(colNameValue, tableName);
		Entry entryToCompare;
		FileInputStream fis = null;
		ObjectInputStream in = null;
		
		
		for (int i = 0; i < this.numPages ; i++){
			fis = new FileInputStream("./Data/" + tableName + "" + i + ".ser");
			in = new ObjectInputStream(fis);
			Page p = (Page) in.readObject();
			int entryCount = getEntryCount(p.getData());
			 
			Entry lastEntry = p.getData()[entryCount-1];
			int q = compareEntries(entry, lastEntry);
			 if(q == -1) {
				for (int qq = 0; qq < entryCount  ; qq++){	
					entryToCompare = p.getData()[qq];
					int qqq = compareEntries(entry, entryToCompare); 
					if(qqq == 0) {
						p.getData()[qq] = edit(entry, entryToCompare);
						p.updatePage();
						return;
					}
				}
			}
			 else if(q == 0) {
				 p.getData()[entryCount - 1] = edit(entry, lastEntry);
				 p.updatePage();
				 return;
			 }
		}
	}

	//helper method that checks whether the table contains the entry or not
	//returns true if it exists else false
	public boolean searchTable(Entry entry) throws IOException, ClassNotFoundException {
		FileInputStream fis = null;
		ObjectInputStream in = null;
		for(int i = 0; i < numPages; i++) {
			fis = new FileInputStream("./Data/" + tableName + "" + i + ".ser");
			in = new ObjectInputStream(fis);
			Page p = (Page) in.readObject();
			int entryCount = getEntryCount(p.getData());
			Entry entry1;
			if(entryCount == 0) {
				fis.close();
				in.close();
				return false;			
			}
			entry1 = p.getData()[entryCount - 1];
			int comp = compareEntries(entry, entry1);
			if(comp == -1) {
				for(int j = entryCount - 2;j >= 0;j--) {
					Entry entry2 = p.getData()[j];
					int comp2 = compareEntries(entry, entry2);
					if(comp2 == 1) {
						fis.close();
						in.close();
						return false;
					}
					else if(comp2 == 0){
						fis.close();
						in.close();
						return true;
					}
				}
			}
			else if(comp == 0) {
				fis.close();
				in.close();
				return true;
			}
		}
		return false;
	}
	public int Search2(Entry entry) throws IOException, ClassNotFoundException{
		FileInputStream fis = null;
		ObjectInputStream in = null;
		for(int i = 0; i < numPages; i++) {
			fis = new FileInputStream("./Data/" + tableName + "" + i + ".ser");
			in = new ObjectInputStream(fis);
			Page p = (Page) in.readObject();
			int entryCount = getEntryCount(p.getData());
			Entry entry1;
			entry1 = p.getData()[entryCount - 1];
			int comp = compareEntries(entry, entry1);
			if(comp == -1) {
				for(int j = entryCount - 2;j >= 0;j--) {
					Entry entry2 = p.getData()[j];
					int comp2 = compareEntries(entry, entry2);


					if(comp2 == 0){
						fis.close();
						in.close();
						return j;
					}
				}
			}
			else if(comp == 0) {
				fis.close();
				in.close();
				return entryCount - 1;
			}
		}
		return -1;
	}

	public void delete(Hashtable<String, Object> colNameType) throws IOException, ClassNotFoundException {

		for (Enumeration<String> columns = colNameType.keys(); columns.hasMoreElements();) {
			if (columns.nextElement().equals(key)) {
				Entry row = new Entry(colNameType, tableName);

				try {
					if (searchTable(row)) {
						FileInputStream fis = null;
						ObjectInputStream in = null;
						int page = -1;
						for (int i = 0; i < numPages; i++) {
							int index = Search2(row);
							fis = new FileInputStream("./Data/" + tableName + "" + i + ".ser");
							in = new ObjectInputStream(fis);
							Page p = (Page) in.readObject();
							int entryCount = getEntryCount(p.getData());
							if(index != -1) {
								if(p.getData()[index].getRow().get(keyIndex).equals(colNameType.get(key))) {
									p.getData()[index] = null;
									p.updatePage();
									page=i;
								}
							}
							else {
								index = 0;
							}
							for(int x = index;x<entryCount - 1;x++){
								if(i != page && page != -1 && x == index){
									Entry temp1 = p.getData()[0];
									System.out.println("i= + " + i);
									System.out.println(temp1.getRow().get(2));
									insert(temp1);
									p.getData()[0] = null;
								}
								p.getData()[x] = p.getData()[x + 1];
								p.getData()[x+1] = null;
								p.updatePage();
							}

						}
						updateSer();
					}
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}

	}

	public int compareEntries(Entry entry1,Entry entry2){
		int q = 0;
		if(entry2 == null) {
			return 1;
		}
		else if(keyType.equals("java.lang.String")){
			q = ((String) entry1.getRow().get(keyIndex)).compareTo((String) entry2.getRow().get(keyIndex));
		}
		else if(keyType.equals("java.lang.Integer")){	
			q =  ((Integer) entry1.getRow().get(keyIndex)).compareTo((Integer) entry2.getRow().get(keyIndex));
		}
		else if(keyType.equals("java.lang.Double")){
			q = ((Double) entry1.getRow().get(keyIndex)).compareTo((Double) entry2.getRow().get(keyIndex));
		}
		else {
			q = ((Date) entry1.getRow().get(keyIndex)).compareTo((Date) entry2.getRow().get(keyIndex));
		}
		return q;
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
	public static int getEntryCount(Entry[] entries) {
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

	public String getKeyType() {
		return keyType;
	}

	public void setKeyType(String keyType) {
		this.keyType = keyType;
	}

	public String getKey() {
		return key;
	}

}
