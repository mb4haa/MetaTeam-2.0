package metaTeam;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;

@SuppressWarnings("serial")
public class Entry implements Serializable{

	private ArrayList<Object> row;
	private ArrayList<String> order;

	//Constructor for the entry class adding the data to the row ArrayList
	Entry(Hashtable<String,Object> entry,String tablename){

		row = new ArrayList<Object>();
		FileInputStream fir = null;
		ObjectInputStream in = null;

		//Loading the table to get the order at which the data should be inserted
		try {
			fir = new FileInputStream(new File("./Data/" + tablename + ".ser"));
			in = new ObjectInputStream(fir);
			Table t = (Table) in.readObject();
			order = t.getOrder();
			in.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		//Inserting the data in the correct order
		for (String col : order) {
			Object curr = entry.get(col);
			row.add(curr);
		}
		//adding a final column containing the time at which the entry was made
		Date d = new Date();
		row.add(d);
	}

	public ArrayList<Object> getRow() {
		return row;
	}

	public void setRow(ArrayList<Object> row) {
		this.row = row;
	}


}
