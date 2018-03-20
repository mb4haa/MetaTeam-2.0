package metaTeam;

import java.io.File;
import java.io.FileInputStream;
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
public class Table implements Serializable {

	private String key;
	private int numPages;
	private ArrayList<String> order;
	private String tableName;
	private int keyIndex;
	private String keyType;
	private ArrayList<String> indexedCols;

	// Constructor for the Table Class, initializes all the object attributes and
	// creates a starter page for entries
	public Table(String key, String tableName, Hashtable<String, String> colName) throws IOException {
		this.order = new ArrayList<String>();
		this.key = key;
		this.tableName = tableName;
		this.numPages = 0;
		this.setKeyType(colName.get(key));
		this.indexedCols = new ArrayList<String>();

		setKeyIndex(colName);
		createPage(tableName);
		updateSer();
	}

	// helper Method for Creating new Tables
	public void createPage(String tableName) throws IOException {
		String pageName = "" + tableName + this.numPages;
		Page p = new Page(pageName);

		// Writing the new Page into a file
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

	// helper Method for inserting new entries into the Table
	// **Unfinished**
	public void insert(Entry entry) throws IOException, ClassNotFoundException {
		FileInputStream fis = null;
		ObjectInputStream in = null;
		int pageCount = this.numPages - 1;
		fis = new FileInputStream("./Data/" + tableName + pageCount + ".ser"); // change page
		in = new ObjectInputStream(fis);
		Page p = (Page) in.readObject();
		int entryCount = getEntryCount(p.getData());

		if (pageCount == 0 && entryCount == 0) {
			p.insert(entry, 0);
			fis.close();
			in.close();
			return;
		}
		for (int i = 0; i < this.numPages; i++) {
			fis = new FileInputStream("./Data/" + tableName + "" + i + ".ser");
			in = new ObjectInputStream(fis);
			p = (Page) in.readObject();
			entryCount = getEntryCount(p.getData());
			Entry lastEntry = p.getData()[entryCount - 1];
			int q = compareEntries(entry, lastEntry);
			if (q == -1) {
				for (int qq = 0; qq < entryCount; qq++) {
					Entry entryq = p.getData()[qq];
					if (compareEntries(entry, entryq) == -1) {
						p.getData()[qq] = entry;
						System.out.println(entry.getRow().get(2));
						entry = entryq;
						p.updatePage();
						System.out.println("Mariam");
					}
				}
				break;
			} else if (getEntryCount(p.getData()) != p.getData().length) {
				p.insert(entry, entryCount);
				fis.close();
				in.close();
				break;
			} else if (i == this.numPages - 1 && getEntryCount(p.getData()) == p.getData().length) {
				createPage(tableName);
				int newPage = i + 1;
				fis = new FileInputStream("./Data/" + tableName + newPage + ".ser");
				in = new ObjectInputStream(fis);
				p = (Page) in.readObject();
				p.insert(entry, 0);
				fis.close();
				in.close();
				break;
			}
		}
		fis.close();
		in.close();
		updateSer();
		if (!searchTable(entry)) {
			insert(entry);
		}
	}

	public static Entry edit(Entry newEntry, Entry oldEntry) {
		int count = 0;
		for (Object obj : newEntry.getRow()) {
			if (obj != null) {
				oldEntry.getRow().set(count, obj);
			}
			count++;
		}
		int timeIndex = oldEntry.getRow().size();
		oldEntry.getRow().set(timeIndex - 1, newEntry.getRow().get(timeIndex - 1));
		return oldEntry;
	}

	public void update(Hashtable<String, Object> colNameValue) throws ClassNotFoundException, IOException {
		Entry entry = new Entry(colNameValue, tableName);
		Entry entryToCompare;
		FileInputStream fis = null;
		ObjectInputStream in = null;

		for (int i = 0; i < this.numPages; i++) {
			fis = new FileInputStream("./Data/" + tableName + "" + i + ".ser");
			in = new ObjectInputStream(fis);
			Page p = (Page) in.readObject();
			int entryCount = getEntryCount(p.getData());

			Entry lastEntry = p.getData()[entryCount - 1];
			int q = compareEntries(entry, lastEntry);
			if (q == -1) {
				for (int qq = 0; qq < entryCount; qq++) {
					entryToCompare = p.getData()[qq];
					int qqq = compareEntries(entry, entryToCompare);
					if (qqq == 0) {
						p.getData()[qq] = edit(entry, entryToCompare);
						p.updatePage();
						in.close();
						break;
					}
				}
			} else if (q == 0) {
				p.getData()[entryCount - 1] = edit(entry, lastEntry);
				p.updatePage();
				in.close();
				break;
			}
		}
	}

	public void delete(Hashtable<String, Object> colNameValue) throws IOException, ClassNotFoundException {
		FileInputStream fis = null;
		ObjectInputStream in = null;

		Enumeration<String> e = colNameValue.keys();
		String element = e.nextElement();
		int index = this.getIndex(element);
		Object value = colNameValue.get(element);
		for (int i = 0; i < this.numPages; i++) {
			fis = new FileInputStream("./Data/" + tableName + "" + i + ".ser");
			in = new ObjectInputStream(fis);
			Page p = (Page) in.readObject();
			int entryCount = getEntryCount(p.getData());

			for (int j = 0; j < entryCount; j++) {
				Entry entry = p.getData()[j];
				if (compareObjects(entry.getRow().get(index), value)) {
					Enumeration<String> e2 = colNameValue.keys();
					boolean flag = true;
					e2.nextElement();
					while (e2.hasMoreElements() && flag) {
						String element2 = e2.nextElement();
						Object value2 = colNameValue.get(element2);
						int index2 = getIndex(element2);
						if (!compareObjects(entry.getRow().get(index2), value2)) {
							flag = false;
						}
					}
					if (flag) {
						p.getData()[j] = null;
						p.updatePage();
						while (j < entryCount - 1) {
							p.getData()[j] = p.getData()[j + 1];
							p.getData()[j + 1] = null;
							p.updatePage();
							j++;
						}
						j = 0;
						entryCount = getEntryCount(p.getData());
					}
				}
			}
		}
	}

	public static boolean compareObjects(Object obj1, Object obj2) {
		if (obj1 == null) {
			return false;
		}
		if (obj1.getClass().getName().equals("java.lang.String")) {
			if (((String) obj1).equals((String) obj2)) {
				return true;
			}
		} else if (obj1.getClass().getName().equals("java.lang.Integer")) {
			if (((Integer) obj1).equals((Integer) obj2)) {
				return true;
			}
		} else if (obj1.getClass().getName().equals("java.lang.Double")) {
			if (((Double) obj1).equals((Double) obj2)) {
				return true;
			}
		} else if (obj1.getClass().getName().equals("java.util.Date")) {
			if (((Date) obj1).equals((Date) obj2)) {
				return true;
			}
		}
		return false;
	}

	public int getIndex(String col) {
		int count = -1;
		for (String column : order) {
			count++;
			if (column.equals(col)) {
				return count;
			}
		}
		return -1;
	}

	// helper method that checks whether the table contains the entry or not
	// returns true if it exists else false
	public boolean searchTable(Entry entry) throws IOException, ClassNotFoundException {
		FileInputStream fis = null;
		ObjectInputStream in = null;
		for (int i = 0; i < numPages; i++) {
			fis = new FileInputStream("./Data/" + tableName + "" + i + ".ser");
			in = new ObjectInputStream(fis);
			Page p = (Page) in.readObject();
			int entryCount = getEntryCount(p.getData());
			Entry entry1;
			if (entryCount == 0) {
				fis.close();
				in.close();
				return false;
			}
			entry1 = p.getData()[entryCount - 1];
			int comp = compareEntries(entry, entry1);
			if (comp == -1) {
				for (int j = entryCount - 2; j >= 0; j--) {
					Entry entry2 = p.getData()[j];
					int comp2 = compareEntries(entry, entry2);
					if (comp2 == 1) {
						fis.close();
						in.close();
						return false;
					} else if (comp2 == 0) {
						fis.close();
						in.close();
						return true;
					}
				}
			} else if (comp == 0) {
				fis.close();
				in.close();
				return true;
			}
		}
		return false;
	}

	public boolean searchIndexed(Entry entry, String colName, int numPages) throws IOException, ClassNotFoundException {
		FileInputStream fis = null;
		ObjectInputStream in = null;
		for (int i = 0; i <= numPages; i++) {
			fis = new FileInputStream("./Data/" + tableName + colName + "Dense" + i + ".ser");
			in = new ObjectInputStream(fis);
			Page p = (Page) in.readObject();
			int entryCount = getEntryCount(p.getData());
			Entry entry1;
			if (i == numPages && entryCount == 0) {
				fis.close();
				in.close();
				return false;
			}
			entry1 = p.getData()[entryCount - 1];
			int comp = compareIndexed(entry, entry1);
			if (comp < 0) {
				for (int j = entryCount - 2; j >= 0; j--) {
					Entry entry2 = p.getData()[j];
					int comp2 = compareIndexed(entry, entry2);
					if (comp2 >= 1) {
						fis.close();
						in.close();
						return false;
					} else if (comp2 == 0) {
						if ((int) entry.getRow().get(1) == (int) entry2.getRow().get(1)
								&& (int) entry.getRow().get(2) == (int) entry1.getRow().get(2)) {
							fis.close();
							in.close();
							return true;
						}
					}
				}
			} else if (comp == 0) {
				if ((int) entry.getRow().get(1) == (int) entry1.getRow().get(1)
						&& (int) entry.getRow().get(2) == (int) entry1.getRow().get(2)) {
					fis.close();
					in.close();
					return true;
				}
			}
		}
		return false;
	}

	public int Search2(Entry entry) throws IOException, ClassNotFoundException {
		FileInputStream fis = null;
		ObjectInputStream in = null;
		for (int i = 0; i < numPages; i++) {
			fis = new FileInputStream("./Data/" + tableName + "" + i + ".ser");
			in = new ObjectInputStream(fis);
			Page p = (Page) in.readObject();
			int entryCount = getEntryCount(p.getData());
			Entry entry1;
			if (entryCount > 0) {
				entry1 = p.getData()[entryCount - 1];
				int comp = compareEntries(entry, entry1);
				if (comp == -1) {
					for (int j = entryCount - 2; j >= 0; j--) {
						Entry entry2 = p.getData()[j];
						int comp2 = compareEntries(entry, entry2);

						if (comp2 == 0) {
							fis.close();
							in.close();
							return j;
						}
					}
				} else if (comp == 0) {
					fis.close();
					in.close();
					return entryCount - 1;
				}
			}
		}
		fis.close();
		in.close();
		return -1;

	}

	public int compareEntries(Entry entry1, Entry entry2) {
		int q = 0;
		if (entry2 == null) {
			return 1;
		} else if (keyType.equals("java.lang.String")) {
			q = ((String) entry1.getRow().get(keyIndex)).compareTo((String) entry2.getRow().get(keyIndex));
		} else if (keyType.equals("java.lang.Integer")) {
			q = ((Integer) entry1.getRow().get(keyIndex)).compareTo((Integer) entry2.getRow().get(keyIndex));
		} else if (keyType.equals("java.lang.Double")) {
			q = ((Double) entry1.getRow().get(keyIndex)).compareTo((Double) entry2.getRow().get(keyIndex));
		} else {
			q = ((Date) entry1.getRow().get(keyIndex)).compareTo((Date) entry2.getRow().get(keyIndex));
		}
		return q;
	}

	// helper Method for updating the serialized Table file
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

	// helper method that initializes the keyIndex attribute
	// according to the index in the arraylist at which the key is at
	public void setKeyIndex(Hashtable<String, String> colName) {
		for (Enumeration<String> e = colName.keys(); e.hasMoreElements();) {
			String curr = e.nextElement();
			if (curr.equals(key)) {
				keyIndex = order.size();
			}
			order.add(curr);
		}
	}

	// helper Method that counts the number of Entries in any Page
	public static int getEntryCount(Entry[] entries) {
		int count = 0;
		for (Entry entry : entries) {
			if (entry != null) {
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

	public void createIndex(String colName) throws ClassNotFoundException, IOException {
		int index;
		index = -1;
		for (int i = 0; i < order.size(); i++) {
			String col = order.get(i);
			if (col.equals(colName)) {
				index = i;
			}
		}
		if (index == -1) {
			System.out.println("Column doesn't exist");
			return;
		}

		if (!indexedCols.contains(colName)) {
			indexedCols.add(colName);
		}

		String pageName = "" + tableName + colName + "Dense" + "0";
		Page pI = new Page(pageName);

		FileOutputStream fos = null;
		ObjectOutputStream out = null;
		try {
			fos = new FileOutputStream(new File("./Data/" + pageName + ".ser"));
			out = new ObjectOutputStream(fos);
			out.writeObject(pI);
			out.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		for (int i = 0; i < numPages; i++) {
			FileInputStream fp = null;
			ObjectInputStream inn = null;
			fp = new FileInputStream("./Data/" + tableName + "" + i + ".ser");
			inn = new ObjectInputStream(fp);
			Page p = (Page) inn.readObject();
			int entryCount = getEntryCount(p.getData());
			for (int j = 0; j < entryCount; j++) {
				Entry entryq = p.getData()[j];
				Entry tBI = new Entry();
				tBI.getRow().add(entryq.getRow().get(index));
				tBI.getRow().add(i);
				tBI.getRow().add(j);
				insertIndex(tBI, colName, i, index);
				inn.close();
			}
		}
		createTopLevelIndex(colName);
		updateSer();
	}

	public static int compareIndexed(Entry entry1, Entry entry2) {
		int q = 0;
		if (entry2 == null) {
			return 1;
		} else if (entry1.getRow().get(0).getClass().getName().equals("java.lang.String")) {
			q = ((String) entry1.getRow().get(0)).compareTo((String) entry2.getRow().get(0));
		} else if (entry1.getRow().get(0).getClass().getName().equals("java.lang.Integer")) {
			q = ((Integer) entry1.getRow().get(0)).compareTo((Integer) entry2.getRow().get(0));
		} else if (entry1.getRow().get(0).getClass().getName().equals("java.lang.Double")) {
			q = ((Double) entry1.getRow().get(0)).compareTo((Double) entry2.getRow().get(0));
		} else {
			q = ((Date) entry1.getRow().get(0)).compareTo((Date) entry2.getRow().get(0));
		}
		return q;
	}

	public void insertIndex(Entry entry, String colName, int numPages, int index)
			throws IOException, ClassNotFoundException {
		FileInputStream fis = null;
		ObjectInputStream in = null;
		Page p;
		int entryCount;

		for (int i = 0; i <= numPages; i++) {
			fis = new FileInputStream("./Data/" + tableName + colName + "Dense" + i + ".ser");
			in = new ObjectInputStream(fis);
			p = (Page) in.readObject();
			entryCount = getEntryCount(p.getData());
			if (entryCount > 0) {
				Entry lastEntry = p.getData()[entryCount - 1];
				int q = compareIndexed(entry, lastEntry);
				if (q < 0) {
					for (int qq = 0; qq < entryCount; qq++) {
						Entry entryq = p.getData()[qq];
						if (compareIndexed(entry, entryq) <= -1) {
							p.getData()[qq] = entry;
							entry = entryq;
							p.updatePage();
						}
					}
					break;

				} else if (getEntryCount(p.getData()) != p.getData().length) {
					p.insert(entry, entryCount);
					fis.close();
					in.close();
					break;
				}
			} else if (i == numPages && entryCount == 0) {
				p.insert(entry, 0);
				fis.close();
				in.close();
				return;
			}
		}

		fis = new FileInputStream("./Data/" + tableName + colName + "Dense" + numPages + ".ser"); // change page
		in = new ObjectInputStream(fis);
		p = (Page) in.readObject();
		entryCount = getEntryCount(p.getData());
		if (getEntryCount(p.getData()) == p.getData().length) {
			String pageName = "" + tableName + colName + "Dense" + (numPages + 1);
			Page pI = new Page(pageName);
			numPages++;
			FileOutputStream fos = null;
			ObjectOutputStream out = null;
			try {
				fos = new FileOutputStream(new File("./Data/" + pageName + ".ser"));
				out = new ObjectOutputStream(fos);
				out.writeObject(pI);
				out.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		in.close();
		if (!searchIndexed(entry, colName, numPages)) {
			insertIndex(entry, colName, numPages, index);
		}
	}

	public void createTopLevelIndex(String colName) throws ClassNotFoundException, IOException {
		String pageName = "" + tableName + colName + "Brin0";
		BrinPage bP = new BrinPage(pageName);
		int count = 0;
		int sumEntries = 0;
		for(int i = 0;i < numPages; i++) {
			FileInputStream fis = null;
			ObjectInputStream in = null;
			fis = new FileInputStream("./Data/" + tableName+ i + ".ser"); // change page
			in = new ObjectInputStream(fis);
			Page p = (Page) in.readObject();
			int entryCount = getEntryCount(p.getData());
			sumEntries += entryCount;
		}
		
		int numPages = ((sumEntries % 10) == 0)? (sumEntries/10) : (sumEntries/10) + 1;
		System.out.println(numPages);

		// Writing the new Page into a file
		FileOutputStream fos = null;
		ObjectOutputStream out = null;
		try {
			fos = new FileOutputStream(new File("./Data/" + pageName + ".ser"));
			out = new ObjectOutputStream(fos);
			out.writeObject(bP);
			out.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		for (int i = 0; i < numPages; i++) {
			FileInputStream fis = null;
			ObjectInputStream in = null;
			fis = new FileInputStream("./Data/" + tableName + colName + "Dense" + i + ".ser"); // change page
			in = new ObjectInputStream(fis);
			Page p = (Page) in.readObject();
			int entryCount = getEntryCount(p.getData());
			if (entryCount != 0) {
				Entry first = p.getData()[0];
				Entry last = p.getData()[entryCount - 1];
				Entry tBI = new Entry();
				tBI.getRow().add(first.getRow().get(0));
				tBI.getRow().add(last.getRow().get(0));
				tBI.getRow().add(i);
				bP.insert(tBI, count);
				count++;
				bP.updatePage();
				if (count == 15) {
					pageName = "" + tableName + colName + "Brin" + ((i + 1) / 15);
					bP = new BrinPage(pageName);
					try {
						fos = new FileOutputStream(new File("./Data/" + pageName + ".ser"));
						out = new ObjectOutputStream(fos);
						out.writeObject(bP);
						out.close();
						count = 0;
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}
			}
			in.close();
		}
	}

	public void updateIndexes() throws IOException, ClassNotFoundException {
		for (String col : indexedCols) {
			
			createIndex(col);
		}
	}

	public ArrayList<String> getIndexedCols() {
		return indexedCols;
	}

}
