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

	private static String[] acceptedTypes = {"java.lang.Integer","java.lang.String", "java.lang.Double", "java.util.Date"};

	//Requested Method for creating new Tables
	public static void createTable(String tableName, String clusteringKey, Hashtable<String,String> colNameType) throws DBAppException, IOException {
		FileWriter fWriter = new FileWriter("./MetaData.csv" , true);
		BufferedWriter bw = new BufferedWriter(fWriter);
		String toBeAdded = "";

		//This Loop checks whether the inputs given to create the Table are of valid types using helper method checkType
		//if an input of invalid type is found the user is notified and the method is exited
		for(Enumeration<String> e = colNameType.keys();e.hasMoreElements();) {
			String type = colNameType.get(e.nextElement());
			if(! checkType(type)) {
				System.out.println("Invalid Type:" + type);
				bw.close();
				return;
			}
		}

		if(checkTableExists(tableName)) {
			System.out.println("Table Already Exists");
			bw.close();
			return;
		}
		
		//This loop writes the Table in the metaData File
		for(Enumeration<String> e = colNameType.keys(); e.hasMoreElements();) {
			String colName = e.nextElement();
			String colType = colNameType.get(colName);


			toBeAdded = tableName + "," + colName + "," + colType + ",";

			//The if condition checks whether the column is the key or not, Writes the first True|False accordingly
			//The second entry here will always be false unless user creates the index using another method to be implemented in minor-milestone 3
			if(colName.equals(clusteringKey)) {
				toBeAdded += "True,False";
			}
			else {
				toBeAdded += "False,False";
			}
			bw.append(toBeAdded + "\n");
		}
		bw.close();

		//A new Table object is instantiated with the table name supplied by the user
		new Table(clusteringKey, tableName, colNameType);
	}

	//Requested Method for inserting entries into Tables
	public static void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException, IOException, ClassNotFoundException {

		if(! checkTableExists(tableName)) {
			System.out.println("No Table " + tableName + " Exists.");
			return;
		}
		
		//This Loop checks Whether the inputs given by the user are of valid Types according to the columns in the table
		//Using a helper method called checkInputType
		for(Enumeration<String> e = colNameValue.keys();e.hasMoreElements();) {
			String curr = e.nextElement();
			if(! checkInputType(tableName, curr, colNameValue.get(curr))) {
				System.out.println("Wrong Input type for " + curr);
				return;
			}
		}

		//Loading the table so we can insert the entry
		FileInputStream fis = null;
		ObjectInputStream in = null;
		fis = new FileInputStream("./Data/" + tableName + ".ser");
		in = new ObjectInputStream(fis);
		Table t = (Table) in.readObject();
		in.close();

		//Writing the entry and inserting into the table
		Entry entry = new Entry(colNameValue, tableName);
		if(t.searchTable(entry)){
			System.out.println("Primary key exists");
			return;
		}

		t.insert(entry);
	}

//	//Requested Method for deleting entries from Tables
//	public void deleteFromTable(String tableName, Hashtable<String,Object> colNameType) throws DBAppException, IOException {
//		FileInputStream fis = null;
//		ObjectInputStream in = null;
//		Table t;
//		try {
//			fis = new FileInputStream(tableName);
//			in = new ObjectInputStream(fis);
//			t = (Table) in.readObject();
//			in.close();
//
//			//			for (int i=0; i<=t.getPages().size(); i++) {
//			//				for (int j=0; j<=t.getPages().get(i).getData().length; j++) {
//			//					for (int k=0; k<t.getPages().get(i).getData()[j].getRow().size(); k++)
//			//						if (t.getPages().get(i).getData()[j].getRow().get(k).equals(colNameType))
//			//							t.getPages().get(i).getData()[j].getRow().remove(k);   }}
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		}
//	}

	//helper Method for checking the input types while creating new Tables
	public static boolean checkType(String type) {
		boolean ret = false;
		for (String acc : acceptedTypes) {
			if(acc.equals(type)) {
				ret = true;
			}
		}
		return ret;
	}

	//helper Method for checking input types while inserting into Tables
	public static boolean checkInputType(String tableName, String colName, Object o) throws IOException {
		boolean ret = false;
		FileReader fr = new FileReader("./MetaData.csv");
		BufferedReader br = new BufferedReader(fr);
		String curr = br.readLine();
		while(curr != null) {
			if(curr.contains("" + tableName + "," + colName)) {
				String type = o.getClass().getName();
				if(curr.contains(type)) {
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
		while(curr != null) {
			if(curr.contains("" + tableName)) {
				return true;
			}
			curr = br.readLine();
		}
		br.close();
		return ret;
	}

	//Main Method for Testing
	public static void main(String[] args) throws IOException, DBAppException, ClassNotFoundException {
		String strTableName = "Student";
		Hashtable<String, String> htblColNameType = new Hashtable<String, String>( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		createTable( strTableName, "id", htblColNameType );

/*		Hashtable htblColNameValue = new Hashtable( );
		htblColNameValue.put("id", new Integer( 2343432 ));
		htblColNameValue.put("name", new String("Ahmed Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.95 ) );
		insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 453455 ));
		htblColNameValue.put("name", new String("Ahmed Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.95 ) );
		insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 5674567 ));
		htblColNameValue.put("name", new String("Dalia Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.25 ) );
		insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 23498 ));
		htblColNameValue.put("name", new String("John Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 78452 ));
		htblColNameValue.put("name", new String("Zaky Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.88 ) );
		insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( ); */
		for(int i = 100; i > 0; i--) {
			Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>( );
			htblColNameValue.put("id", new Integer( i ));
			htblColNameValue.put("name", new String("Ahmed Noor" ) );
			htblColNameValue.put("gpa", new Double(0.95));
			insertIntoTable( strTableName , htblColNameValue );
			htblColNameValue.clear();
		}
	}

}
