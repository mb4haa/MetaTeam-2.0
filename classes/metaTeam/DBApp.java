package metaTeam;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Enumeration;
import java.util.Hashtable;

public class DBApp {

	private static String[] acceptedTypes = { "java.lang.Integer", "java.lang.String", "java.lang.Double",
			"java.util.Date" };

	// Requested Method for creating new Tables
	public static void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType)
			throws DBAppException, IOException {
		FileWriter fWriter = new FileWriter("./MetaData.csv", true);
		BufferedWriter bw = new BufferedWriter(fWriter);
		String toBeAdded = "";

		// This Loop checks whether the inputs given to create the Table are of valid
		// types using helper method checkType
		// if an input of invalid type is found the user is notified and the method is
		// exited
		for (Enumeration<String> e = colNameType.keys(); e.hasMoreElements();) {
			String type = colNameType.get(e.nextElement());
			if (!checkType(type)) {
				System.out.println("Invalid Type:" + type);
				bw.close();
				return;
			}
		}

		if (checkTableExists(tableName)) {
			System.out.println("Table Already Exists");
			bw.close();
			return;
		}

		// This loop writes the Table in the metaData File
		for (Enumeration<String> e = colNameType.keys(); e.hasMoreElements();) {
			String colName = e.nextElement();
			String colType = colNameType.get(colName);

			toBeAdded = tableName + "," + colName + "," + colType + ",";

			// The if condition checks whether the column is the key or not, Writes the
			// first True|False accordingly
			// The second entry here will always be false unless user creates the index
			// using another method to be implemented in minor-milestone 3
			if (colName.equals(clusteringKey)) {
				toBeAdded += "True,False";
			} else {
				toBeAdded += "False,False";
			}
			bw.append(toBeAdded + "\n");
		}
		bw.close();

		// A new Table object is instantiated with the table name supplied by the user
		new Table(clusteringKey, tableName, colNameType);
	}

	// Requested Method for inserting entries into Tables
	public static void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue)
			throws DBAppException, IOException, ClassNotFoundException {

		if (!checkTableExists(tableName)) {
			System.out.println("No Table " + tableName + " Exists.");
			return;
		}

		// This Loop checks Whether the inputs given by the user are of valid Types
		// according to the columns in the table
		// Using a helper method called checkInputType
		for (Enumeration<String> e = colNameValue.keys(); e.hasMoreElements();) {
			String curr = e.nextElement();
			if (!checkInputType(tableName, curr, colNameValue.get(curr))) {
				System.out.println("Wrong Input type for " + curr);
				return;
			}
		}

		// Loading the table so we can insert the entry
		FileInputStream fis = null;
		ObjectInputStream in = null;
		fis = new FileInputStream("./Data/" + tableName + ".ser");
		in = new ObjectInputStream(fis);
		Table t = (Table) in.readObject();
		in.close();

		// Writing the entry and inserting into the table
		Entry entry = new Entry(colNameValue, tableName);
		if (t.searchTable(entry)) {
			System.out.println("Primary key exists");
			return;
		}

		t.insert(entry);
		fis.close();
		in.close();
		t.updateIndexes();
	}

	public static void updateTable(String tableName, Hashtable<String, Object> colNameValue)
			throws IOException, ClassNotFoundException {
		FileInputStream fis;
		ObjectInputStream in;
		fis = new FileInputStream("./Data/" + tableName + ".ser");
		in = new ObjectInputStream(fis);
		Table table = (Table) in.readObject();

		if (!colNameValue.containsKey(table.getKey())) {
			System.out.println("Key Not given");
			in.close();
			return;
		}
		Entry entry = new Entry(colNameValue, tableName);
		if (!table.searchTable(entry)) {
			System.out.println("No Entry with primary key found");
			in.close();
			return;
		}
		table.update(colNameValue);
		fis.close();
		in.close();
		table.updateIndexes();
	}

	// Requested Method for deleting entries from Tables
	public static void deleteFromTable(String tableName, Hashtable<String, Object> colNameType)
			throws DBAppException, IOException {
		FileInputStream fis = null;
		ObjectInputStream in = null;
		Table t;
		try {
			fis = new FileInputStream("./data/" + tableName + ".ser");
			in = new ObjectInputStream(fis);
			t = (Table) in.readObject();
			in.close();
			t.delete(colNameType);
			fis.close();
			in.close();
			t.updateIndexes();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	// helper Method for checking the input types while creating new Tables
	public static boolean checkType(String type) {
		boolean ret = false;
		for (String acc : acceptedTypes) {
			if (acc.equals(type)) {
				ret = true;
			}
		}
		return ret;
	}

	// helper Method for checking input types while inserting into Tables
	public static boolean checkInputType(String tableName, String colName, Object o) throws IOException {
		boolean ret = false;
		FileReader fr = new FileReader("./MetaData.csv");
		BufferedReader br = new BufferedReader(fr);
		String curr = br.readLine();
		while (curr != null) {
			if (curr.contains("" + tableName + "," + colName)) {
				String type = o.getClass().getName();
				if (curr.contains(type)) {
					ret = true;
				}
			}
			curr = br.readLine();
		}
		br.close();
		return ret;
	}

	public static boolean checkTableExists(String tableName) throws IOException {
		boolean ret = false;
		FileReader fr = new FileReader("./MetaData.csv");
		BufferedReader br = new BufferedReader(fr);
		String curr = br.readLine();
		while (curr != null) {
			if (curr.contains("" + tableName)) {
				br.close();
				return true;
			}
			curr = br.readLine();
		}
		br.close();
		return ret;
	}

	public static void changeIndexed(String tableName, String colName) throws IOException {
		FileReader fr = new FileReader("./MetaData.csv");
		BufferedReader br = new BufferedReader(fr);
		String curr = br.readLine();
		StringBuffer tempBuffer = new StringBuffer();
		while (curr != null) {
			if (curr.contains("" + tableName + "," + colName)) {
				String[] temp = curr.split(",");
				if (temp[4].equals("True")) {
					System.out.println("Already Indexed");
					br.close();
					return;
				}
				String line = tableName + "," + colName + "," + temp[2] + "," + "False,True";
				tempBuffer.append(line);
				tempBuffer.append("\n");
			} else {
				tempBuffer.append(curr);
				tempBuffer.append('\n');
			}
			curr = br.readLine();
		}
		br.close();
		FileWriter fileOut = new FileWriter("MetaData.csv");
		fileOut.write(tempBuffer.toString());
		fileOut.close();

	}

	public static void createBRINIndex(String strTableName, String strColName)
			throws DBAppException, IOException, ClassNotFoundException {
		if (!checkTableExists(strTableName)) {
			System.out.println("Table Doesn't Exist");
			return;
		}

		changeIndexed(strTableName, strColName);

		FileInputStream fis = null;
		ObjectInputStream in = null;
		fis = new FileInputStream("./Data/" + strTableName + ".ser");
		in = new ObjectInputStream(fis);
		Table t = (Table) in.readObject();

		t.createIndex(strColName);

		in.close();
	}

	// Main Method for Testing
	public static void main(String[] args) throws IOException, DBAppException, ClassNotFoundException {
		String strTableName = "Student";
//		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
//		htblColNameType.put("id", "java.lang.Integer");
//		htblColNameType.put("name", "java.lang.String");
//		htblColNameType.put("gpa", "java.lang.Double");
//		createTable(strTableName, "id", htblColNameType);
//
//		for (int i = 0; i < 200; i++) {
//			Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
//			htblColNameValue.put("id", new Integer(i));
//			htblColNameValue.put("name", new String("Ahmed Noor"));
//			htblColNameValue.put("gpa", new Double(0.95));
//			insertIntoTable(strTableName, htblColNameValue);
//			htblColNameValue.clear();
//		}
//		
//		for (int i = 0; i < 200; i += 10) {
//			Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
//			htblColNameValue.put("id", new Integer(i));
//			htblColNameValue.put("name", new String("Mahmoud Bahaa"));
//			htblColNameValue.put("gpa", new Double(0.95));
//			updateTable(strTableName, htblColNameValue);
//			htblColNameValue.clear();
//		}
//		
//		 for(int i = 0; i < 200; i+= 5) {
//		 Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
//		 htblColNameValue.put("id", new Integer( i ));
//		 htblColNameValue.put("gpa", new Double(1.00));
//		 updateTable( strTableName , htblColNameValue );
//		 htblColNameValue.clear();
//		 }

//		 Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
//		 htblColNameValue.put("gpa", new Double(1.00));
//		 deleteFromTable(strTableName, htblColNameValue);

//		createBRINIndex(strTableName, "name");
//		
//		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
//		htblColNameValue.put("id", new Integer(199));
//		htblColNameValue.put("name", new String("Ziad Noor"));
//		htblColNameValue.put("gpa", new Double(0.95));
//		updateTable(strTableName, htblColNameValue);
//		htblColNameValue.clear();
//		
//		Hashtable<String, Object> htblColNameValue2 = new Hashtable<String, Object>();
//		htblColNameValue2.put("id", new Integer(200));
//		htblColNameValue2.put("name", new String("Lame Noor"));
//		htblColNameValue2.put("gpa", new Double(0.95));
//		insertIntoTable(strTableName, htblColNameValue2);
//		htblColNameValue2.clear();

		Hashtable<String, Object> htblColNameValue3 = new Hashtable<String, Object>();
		 htblColNameValue3.put("name", new String("Mahmoud Bahaa"));
		 deleteFromTable(strTableName, htblColNameValue3);
	}

}
