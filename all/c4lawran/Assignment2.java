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
  public boolean connectDB(String URL, String username, String password){
	try {
        	connection = DriverManager.getConnection(URL, username, password);
		return true;
	} catch (SQLException e) {
		return false;
	}
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
	try {
        	connection.close();
		return true;
	} catch (SQLException e) {
		return false;
	}
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
	try {
		ps = connection.prepareStatement("INSERT INTO A2.country VALUES (?, ?, ?, ?);"); 
		ps.setInt(1, cid);
		ps.setString(2, name);
		ps.setInt(3, height);
		ps.setInt(4, population);


		ps.executeUpdate();


		return true;
	} catch (SQLException e) {
		System.out.println(e);
		return false;
	}
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	try {
	ps = connection.prepareStatement("SELECT COUNT(DISTINCT cid) AS count FROM A2.oceanaccess WHERE oid=?;"); 
	ps.setInt(1, oid);
	
	rs = ps.executeQuery();

	rs.next();
	return rs.getInt("count");
	} catch (SQLException e) {
	return -1;  
	}
  }
   
  public String getOceanInfo(int oid){
	try{
	ps = connection.prepareStatement("SELECT * FROM A2.ocean WHERE oid=?;"); 
	ps.setInt(1, oid);
	
	rs = ps.executeQuery();

	String oceanInfo = "";
	while (rs.next()) {
		oceanInfo = rs.getInt("oid") + ":" + rs.getString("oname") + ":" + rs.getInt("depth");
	}

	return oceanInfo;

	} catch (SQLException e) {
	return "";
	}
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	try {
	ps = connection.prepareStatement("UPDATE A2.hdi SET hdi_score=? WHERE cid=? AND year=?;"); 
	ps.setFloat(1, newHDI);
	ps.setInt(2, cid);
	ps.setInt(3, year);
	
	ps.executeUpdate();

	return true;

	} catch (SQLException e) {
	return false;
	}
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	try {
	ps = connection.prepareStatement("DELETE FROM A2.neighbour WHERE (country=? AND neighbor=?) OR (country=? AND neighbor=?);"); 
	ps.setInt(1, c1id);
	ps.setInt(2, c2id);
	ps.setInt(3, c2id);
	ps.setInt(4, c1id);
	
	ps.executeUpdate();

	return true;

	} catch (SQLException e) {
	return false;
	}

  }
  
  public String listCountryLanguages(int cid){
	String query = "SELECT lid, lname, SUM(lpercentage*population) AS population FROM A2.language JOIN A2.country USING (cid) WHERE cid=? GROUP BY lid, lname ORDER BY population;";
	try {
	ps = connection.prepareStatement(query);
	ps.setInt(1, cid);

	rs = ps.executeQuery();

	StringBuilder languages = new StringBuilder();

	if (rs.next()) {
		languages.append(rs.getInt("lid") + ":" + rs.getString("lname") + ":" + rs.getInt("population"));

	}
	while (rs.next()) {
		languages.append("#" + rs.getInt("lid") + ":" + rs.getString("lname") + ":" + rs.getInt("population"));
	}
	if (languages.length() != 0) {
		languages.delete(languages.length()-2, languages.length()-2);
	}
	return languages.toString();
	} catch (SQLException e) {
	return "";
	}
  }
  
  public boolean updateHeight(int cid, int decrH){
	try {
	ps = connection.prepareStatement("SELECT height FROM A2.country WHERE cid=?;");
	ps.setInt(1, cid);
	rs = ps.executeQuery();
	rs.next();
	int height = rs.getInt("height");

	ps = connection.prepareStatement("UPDATE A2.country SET height=? WHERE cid=?");
	ps.setInt(1, height-decrH);
	ps.setInt(2, cid);
	ps.executeUpdate();

	return true;
	} catch (SQLException e) {
    	return false;
	}
  }
    
  public boolean updateDB(){
	try {
	ps = connection.prepareStatement("DROP TABLE IF EXISTS A2.mostPopulousCountries;");
	ps.executeUpdate();

	ps = connection.prepareStatement("SELECT cid, cname INTO A2.mostPopulousCountries FROM A2.country WHERE population>100000000 ORDER BY cid ASC;");
	ps.executeUpdate();

	return true;

	} catch (SQLException e) {
	return false;    
	}
  }
  
}
