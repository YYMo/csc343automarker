import java.sql.*;

public class Assignment2 {
    
  // A connection to the database  
  Connection connection;
  
  // Statement to run queries
  Statement sql;
  
  // Prepared Statement
  PreparedStatement ps;
  
  // Resultset for the query
  ResultSet rs;
  
  //CONSTRUCTOR
  Assignment2(){
	try {
		Class.forName("org.postgresql.jdbc.Driver");
	}
	catch (ClassNotFoundException nf) {
		
	}
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	try {
		connection = DriverManager.getConnection ("jdbc:postgresql://" + URL, username, password);
	}
	catch (SQLException ex) {
		return false;
    	}
    	return true;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
	try {
		connection.close();
	}
	catch (SQLException ex) {
		return false;
    	}
    	return true;    
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
	int count = 0;
  
	try {
		// Check to see if the country with the given cid exists
		ps = connection.prepareStatement ("SELECT count(*) FROM a2.country WHERE cid=?");
		ps.setInt(1, cid);
		rs = ps.executeQuery();
		rs.next();
		count = rs.getInt(1);
		
		// If a country with the given cid doesn't exist, then insert the new information
		// into the country table
		if (count == 0) {
			ps = connection.prepareStatement ("INSERT INTO a2.country VALUES (?, ?, ?, ?)");
			ps.setInt(1, cid);
			ps.setString(2, name);
			ps.setInt(3, height);
			ps.setInt(4, population);
			ps.executeUpdate();
			return true;
		}
		else
			return false;
	}
	catch (SQLException ex) {
		return false;
   	}
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	try {
		ps = connection.prepareStatement ("SELECT count(*) FROM a2.oceanAccess WHERE oid=?");
		ps.setInt(1, oid);
		rs = ps.executeQuery();
		if (rs.next())
			return (rs.getInt(1));
		else
			return -1;
	}
	catch (SQLException ex) {
		return -1;
	}
  }
   
  public String getOceanInfo(int oid){
	String oceanInfo;

	try {
		ps = connection.prepareStatement ("SELECT * from a2.ocean WHERE oid=?");
		ps.setInt(1, oid);
		rs = ps.executeQuery();
		// Try to put the string together if the specified ocean exists
		if (rs.next())
			oceanInfo = rs.getInt(1) + ":" + rs.getString(2) + ":" + rs.getInt(3);
		else
			oceanInfo = "";
		return oceanInfo;
	}
	catch (SQLException ex) {
		return "";
	}
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	int result;

	try {
		ps = connection.prepareStatement ("UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? and year = ?");
		ps.setFloat(1, newHDI);
		ps.setInt(2, cid);
		ps.setInt(3, year);
		result = ps.executeUpdate();
		// Return true iff the row with the given cid and year get updated
		if (result == 1)
			return true;
		else
			return false;
	}
	catch (SQLException ex) {
		return false;
	}
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	int result;

	try {
		// Attempts to delete both instances of the neighboring relation inside the neighour table
		ps = connection.prepareStatement ("DELETE FROM a2.neighbour WHERE country=? AND neighbor=?");
		ps.setInt(1, c1id);
		ps.setInt(2, c2id);
		result = ps.executeUpdate();
		ps = connection.prepareStatement ("DELETE FROM a2.neighbour WHERE country=? AND neighbor=?");
		ps.setInt(2, c1id);
		ps.setInt(1, c2id);
		result += ps.executeUpdate();
		// Iff both rows were delete from the table, then return yes
		if (result == 2)
			return true;
		else
			return false;
	}
	catch (SQLException ex) {
		return false;   
	}
   
  }
  
  public String listCountryLanguages(int cid){
	String result = "";

	try {
		ps = connection.prepareStatement ("SELECT c1.cid AS cid, l1.lname AS lname," 							+ "(c1.population*l1.lpercentage) AS population "
						+ "FROM a2.country c1 join a2.language l1 on c1.cid=l1.cid "
						+ "WHERE c1.cid = ? "
						+ "ORDER BY population");
		ps.setInt(1, cid);
		rs = ps.executeQuery();
		while (rs.next()) {
			// Add a #'s to seperate the rows if more than one row is retrieved by the query
			if (result != "")
				result = result + "#";
			result = result + rs.getInt(1) + ":";
			result = result + rs.getString(2) + ":";
			result = result + rs.getFloat(3);
		}
		return result;
	}
	catch (SQLException ex) {
		return "";
	}
  }
  
  public boolean updateHeight(int cid, int decrH){
	int oldHeight;

	try {
		// First, get the old height of the country with the given cid
		ps = connection.prepareStatement ("SELECT height FROM a2.country WHERE cid=?");
		ps.setInt(1, cid);
		rs = ps.executeQuery();
		// If the country with the given cid exists, then update the height of the country
		if (rs.next()) {
			oldHeight = rs.getInt(1);
			ps = connection.prepareStatement ("UPDATE a2.country SET height=" + (oldHeight - decrH)
							+ " WHERE cid=" + cid);
			if (ps.executeUpdate() == 1)
				return true;
		}
			
	}
	catch (SQLException ex) {
		return false;
	}
	return false;
  }
    
  public boolean updateDB(){
	int cid;
	String cname;
	
	try {
		// Create the table mostPopulousCountries first
		ps = connection.prepareStatement ("create table a2.mostPopulousCountries ("
						+ "cid INTEGER, "
						+ "cname VARCHAR(20))");
		ps.executeUpdate();
		ps = connection.prepareStatement ("SELECT cid, cname FROM a2.country " 							+ "WHERE population > 100000000 ORDER BY cid ASC");
		rs = ps.executeQuery();
		while (rs.next()) {
			cid = rs.getInt(1);
			cname = rs.getString(2);
			ps = connection.prepareStatement ("INSERT INTO a2.mostPopulousCountries VALUES"
							+ "(" + cid + ", '" + cname + "')");
			ps.executeUpdate();
		}
		return true;
	}
	catch (SQLException ex) {
		//System.err.println(ex.getMessage());
		//ex.printStackTrace();
		return false;
	}   
  }
  
}
