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
  Assignment2() throws ClassNotFoundException{
	try {
		Class.forName("org.postgresql.Driver");
	}
	catch (ClassNotFoundException e) {
		;
	}
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password) throws SQLException{
      try {
		connection = DriverManager.getConnection(URL, username, password);
		ps = connection.prepareStatement("SET search_path TO A2");	
		ps.execute();
		return true;
	  }
	  catch (SQLException se) {
		return false;
      }
	  
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      	try {
		rs.close();
		ps.close();
		sql.close();
		connection.close();
		return true;
	}
	catch (SQLException ex) {
	return false;
	}   
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) throws SQLException{
	try {
		ps = connection.prepareStatement
		("INSERT INTO country(cid, cname, height, population) VALUES (?, ?, ?, ?);");
		ps.setInt(1, cid);
		ps.setString(2, name);
		ps.setInt(3, height);
		ps.setInt(4, population);
		ps.execute();
		return true;
	}
    catch (SQLException se) {
		return false;
    }
  }
  
  public int getCountriesNextToOceanCount(int oid) throws SQLException{
	try {
		ps = connection.prepareStatement
		("SELECT oid, count(cid) as number FROM oceanAccess WHERE oid = ? GROUP BY oid;");
		ps.setInt (1, oid);
		rs = ps.executeQuery();
		rs.next();
		return rs.getInt("number");
	} 
	catch (SQLException ex) {
		return -1;
	}
  }
   
  public String getOceanInfo(int oid) throws SQLException{
	try {
		ps = connection.prepareStatement("SELECT * FROM ocean WHERE oid = ?;");
		ps.setInt(1, oid);
		rs = ps.executeQuery();
		rs.next();
		return rs.getInt("oid") + ":" + rs.getString("oname") + ":" + rs.getInt("depth");
	}
	catch (SQLException se) {
		return "";
	}
  }

  public boolean chgHDI(int cid, int year, float newHDI) throws SQLException{
   	try {
	ps = connection.prepareStatement(
	"UPDATE hdi SET hdi_score = ? WHERE cid = ? AND year = ?;");
	ps.setFloat(1, newHDI);
	ps.setInt (2, cid);
	ps.setInt (3, year);
	ps.execute();	
	return true;
	}
	catch (SQLException ex) {
	return false;
	}
  }

  public boolean deleteNeighbour(int c1id, int c2id) throws SQLException{
	try {
		ps = connection.prepareStatement("DELETE FROM neighbour WHERE country = ? AND neighbor = ?;");
		ps.setInt(1, c1id);
		ps.setInt(2, c2id);
		ps.execute();
		ps = connection.prepareStatement("DELETE FROM neighbour WHERE country = ? AND neighbor = ?;");
		ps.setInt(1, c2id);
		ps.setInt(2, c1id);
		ps.execute();
		return true;
	}
    catch (SQLException se) {
		return false;
    }       
  }
  
  public String listCountryLanguages(int cid) throws SQLException {
	try {
		ps = connection.prepareStatement(
		"SELECT language.lid AS lid, language.lname AS lname, country.population * language.lpercentage AS population FROM country JOIN language ON country.cid = language.cid WHERE country.cid = ? ORDER BY population;");
		ps.setInt (1, cid);
		rs = ps.executeQuery();
		String bundle = "";
		int counter = 1;
		while (rs.next()) {
			bundle = bundle + "l" + Integer.toString(counter) + rs.getInt("lid") + ":" + "l" + Integer.toString(counter) + rs.getString("lname") + ":" + "l" + Integer.toString(counter) + rs.getFloat("population") + "#";
			counter++;
		}
		return bundle;
	}	
	catch (SQLException ex) {
		return "";
	}
  }
  
  public boolean updateHeight(int cid, int decrH) throws SQLException{
    try {
		ps = connection.prepareStatement("UPDATE country SET height = (SELECT height FROM country WHERE cid = ?) - ? WHERE cid = ?;");
		ps.setInt(1, cid);
		ps.setInt(2, decrH);
		ps.setInt(3, cid);
		ps.execute();
		return true;
	}
	catch (SQLException se) {
		return false;
    } 
  }
    
  public boolean updateDB() throws SQLException{
	  try {
	ps = connection.prepareStatement("CREATE TABLE mostPopulousCountries(cid 		INTEGER			PRIMARY KEY,	cname		VARCHAR(20)		NOT NULL);	INSERT INTO mostPopulousCountries(	SELECT cid, cname	FROM country	WHERE population > 100000000	ORDER BY cid ASC			);");
	ps.execute();
	return true;
	}
	catch (SQLException ex) {
	return false;    
	}  
  }
  
  //public static void main(String args[]) throws Exception{
	//Assignment2 a = new Assignment2();
	//System.out.println(a.connectDB("jdbc:postgresql://localhost:5432/csc343h-c3yangjk", "c3yangjk", ""));
	////System.out.println(a.getCountriesNextToOceanCount(1));
	//System.out.println(a.getOceanInfo(1));
	//System.out.println(a.chgHDI(1, 2009, (float)0.99));
	//a.listCountryLanguages(101);
	//System.out.println(a.updateDB());
  //}
  
}
