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
		Class.forName("org.postgresql.Driver");
	}
	catch (ClassNotFoundException e) {
		System.out.println("Failed to find the JDBC driver");
	}
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password) {
	try {
		connection = DriverManager.getConnection(URL, username, password);
		return true;
	}
	catch (SQLException se) {
		return false;
	}
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB() {
	try{
		connection.close();
		return true;
	}
	catch (SQLException se) {
		return false;
	}
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
	try {	
		//Checks if cid already exists in the table
		int count = 0;
		boolean existsInTable = false;
		ps = connection.prepareStatement("SELECT cid FROM country");
		rs = ps.executeQuery();
		while (rs.next()) {
			if (rs.getInt(1) == cid) {
				existsInTable = true;
				break;
			}
		}
		if (!existsInTable) {
			ps = connection.prepareStatement("INSERT INTO country VALUES (" + cid + ", '" + name + "', " + height + ", " + population + ")");
			count = ps.executeUpdate();
			ps.close();
			rs.close();
			return count == 1;
		}
		ps.close();
		rs.close();
		return false;
	}
	catch (SQLException se) {
		return false;
	}
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	try {
		int counter = 0;
		ps = connection.prepareStatement("SELECT * FROM oceanAccess");
		rs = ps.executeQuery();
		while (rs.next()) {
			if (rs.getInt(2) == oid) {
				counter ++;
			}
		}
		ps.close();
		rs.close();
		return counter;
	}
	catch (SQLException se) {
		return -1;
	}
  }

  public String getOceanInfo(int oid){
	try {
		ps = connection.prepareStatement("SELECT * FROM ocean");
		rs = ps.executeQuery();
		while (rs.next()) {
			if (rs.getInt(1) == oid) {
				String oname = rs.getString(2);
				int depth = rs.getInt(3);
				ps.close();
				rs.close();
				return oid + ":" + oname + ":" + depth;
			}
		}
		ps.close();
		rs.close();
		return "";
	}
	catch (SQLException se) {
		return "";
	}
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	try {
		//0<= newHDI <= 1
		if (newHDI < 0.0 || newHDI > 1.0) {
			return false;
		}
		int count = 0;
		ps = connection.prepareStatement("UPDATE hdi SET hdi_score = " + newHDI + " WHERE cid = " + cid + " AND year = " + year);
		count = ps.executeUpdate();
		ps.close();
		return count == 1;
	}
	catch (SQLException se) {
		return false;
	}
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	try {
		int count = 0;
		int count2 = 0;
		ps = connection.prepareStatement("DELETE FROM neighbour WHERE country = " + c1id + " AND neighbor = " + c2id);
		count = ps.executeUpdate();
		ps = connection.prepareStatement("DELETE FROM neighbour WHERE country = " + c2id + " AND neighbor = " + c1id);
		count2 = ps.executeUpdate();
		ps.close();
		return (count == 1) && (count2 == 1);
	}
	catch (SQLException se) {
		return false;
	}     
  }

  public String listCountryLanguages(int cid){
	try {
		int cidPopulation = 0;
		boolean cidExists = false;
		String result = "";
		ps = connection.prepareStatement("SELECT population FROM country where cid = " + cid);
		rs = ps.executeQuery();
		while (rs.next()) {
			cidExists = true;
			cidPopulation = rs.getInt(1);
		}
		
		if (!cidExists) {
			ps.close();
			rs.close();
			return "";
		}

		ps = connection.prepareStatement("SELECT lid, lname, (language.lpercentage * " + cidPopulation + " / 100) AS population FROM language, country WHERE language.cid = " + cid + " AND language.cid = country.cid ORDER BY population");
		rs = ps.executeQuery();
		while (rs.next()) {
			result += rs.getInt(1) + ":" + rs.getString(2) + ":" + rs.getInt(3) + "#";
		}
		ps.close();
		rs.close();
		if (result.equals("")) {
			return "";
		}
		result = result.substring(0, result.length() - 1); //Get rid of last pound sign (#) at the end
		return result;
	}
	catch (SQLException se) {
		return "";
	}
  }

  public boolean updateHeight(int cid, int decrH){
	try {
		int count = 0;
		ps = connection.prepareStatement("UPDATE country SET height = (country.height - " + decrH + ") WHERE cid = " + cid);
		count = ps.executeUpdate();
		ps.close();
		return count == 1;
	}
	catch (SQLException se) {
		return false;		
	}
  }
   
  public boolean updateDB(){
	try {
		int count = 0;
		ps = connection.prepareStatement("CREATE TABLE IF NOT EXISTS mostPopulousCountries (cid INTEGER PRIMARY KEY, cname VARCHAR(20) NOT NULL)");
		ps.executeUpdate();

		ps = connection.prepareStatement("INSERT INTO mostPopulousCountries (SELECT cid, cname FROM country WHERE population > 100000000 ORDER BY cid ASC)");
		count = ps.executeUpdate();

		//Check we inserted the correct number of tuples
		int over1mInCountry = 0;
		ps = connection.prepareStatement("SELECT cid FROM country WHERE population > 100000000");
		rs = ps.executeQuery();
		while (rs.next()) {
			over1mInCountry ++;
		}
		ps.close();
		rs.close();
		return count == over1mInCountry;
	}
	catch (SQLException se) {
		return false;			
	}   
  }

//jdbc:postgresql://localhost:5432/csc343h-g3aishih
//java -cp /local/packages/jdbc-postgresql/postgresql-8.3-604.jdbc4.jar: Assignment2
}
