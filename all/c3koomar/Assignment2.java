import java.sql.*;

public class Assignment2 {
    
  // A connection to the database  
  Connection connection;
  
  // Statement to run queries
  String queryString;
  
  // Prepared Statement
  PreparedStatement ps;
  PreparedStatement ps2;
  
  // Resultset for the query
  ResultSet rs;
  
  //CONSTRUCTOR
  Assignment2(){
	  try{
	  Class.forName("org.postgresql.Driver");
	  }
	  catch(ClassNotFoundException e){
		  System.out.println("No driver");
	  }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	  try{
		  if(URL == null){
			  URL = "jdbc:postgresql://localhost:5432/csc343h-c3koomar";
		  }
		  if(username == null){
			  username = "c3koomar";
		  }
          connection = DriverManager.getConnection(URL, username, "");
	  }
	  catch (SQLException se)
      {
		  System.out.println(se.getMessage());
          return false;
      }
	  return true;
  }

  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      return false;    
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
	  queryString = "INSERT INTO country VALUES (" + Integer.toString(cid) +
			  ", \'" + name + "\', " + Integer.toString(height) + ", " 
			  + Integer.toString(population) + ");";
	  try{
		  ps = connection.prepareStatement(queryString);
		  ps.executeUpdate(); 
	  }
	  catch(SQLException se)
	  {
		  return false;
	  }
	  return true; 
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	int countryCount = -1;
	queryString = "select COUNT(*) FROM oceanaccess WHERE oid = " + oid + ";";
	try{
		  ps = connection.prepareStatement(queryString);
		  rs = ps.executeQuery(); 
		  rs.next();
		  countryCount = rs.getInt("count");
	  }
	  catch(SQLException se)
	  {
	      System.out.println("getCountriesNextToOcean: " + se.getMessage());
		  return -1;
	  }
	  return countryCount;
  }
   
  public String getOceanInfo(int oid){
   String oceanInfo = "";
   queryString = "SELECT * FROM ocean WHERE oid = " + oid + ";";
   try{
		  ps = connection.prepareStatement(queryString);
		  rs = ps.executeQuery(); 
		  while(rs.next()){
		  oceanInfo += Integer.toString(rs.getInt("oid")) + ":";
		  oceanInfo += rs.getString("oname") + ":";
		  oceanInfo += Integer.toString(rs.getInt("depth"));
		  }
	  }
  catch(SQLException se)
  {
	  System.out.println("getCountriesNextToOcean: " + se.getMessage());
	  return "";
  }
   return oceanInfo;
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	queryString = "SELECT * FROM hdi WHERE cid = " + Integer.toString(cid) + " AND YEAR = "  + Integer.toString(year) + ";";
	try{
		ps = connection.prepareStatement(queryString);
		rs = ps.executeQuery(); 
	
	while(rs.next()){
		queryString = "UPDATE hdi SET hdi_score = " + String.valueOf(newHDI) + 
		" WHERE cid = " + Integer.toString(cid) + " and year = " + Integer.toString(year) + ";";
		  ps = connection.prepareStatement(queryString);
		  rs = ps.executeQuery(); 
		  return true;
	   }
	}
	   catch(SQLException se){
	   return false;
	   }
	return false;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
  try{
		queryString = "DELETE FROM neighbour WHERE country = " + Integer.toString(c1id) + " AND neighbor = " 
		+ Integer.toString(c2id) + ";";
		ps = connection.prepareStatement(queryString);
		ps.executeUpdate(); 
		
		queryString = "DELETE FROM neighbour WHERE country = " + Integer.toString(c2id) + " AND neighbor = " 
		+ Integer.toString(c1id) + ";";
		ps = connection.prepareStatement(queryString);
		ps.executeUpdate(); 
		return true;
	}
	catch(SQLException se)
	{
		System.out.println("deleteNeighbour: " + se.getMessage());
		return false;
	}       
  }
  
  public String listCountryLanguages(int cid){
	queryString = "SELECT lid, lname, population*lpercentage AS number FROM (country NATURAL JOIN (SELECT * FROM language WHERE cid = " + Integer.toString(cid) + ") as foo1) as foo2 ORDER BY population;";
	String result = "";
	try{
		ps = connection.prepareStatement(queryString);
		rs = ps.executeQuery(); 
		while(rs.next()){
			result += rs.getString("lid") + ":";
			result += rs.getString("lname") + ":";
			result += rs.getFloat("number");
			if(!rs.isLast()){
				result += "#"; 
			}
		}
	}
	catch(SQLException se){
		System.out.println("listCountryLanguages: " + se.getMessage());
		return "";
	}
	return result;
  }
  
  public boolean updateHeight(int cid, int decrH){
	queryString = "SELECT * FROM country WHERE cid = " + Integer.toString(cid);
	try{
			ps = connection.prepareStatement(queryString);
			rs = ps.executeQuery();
			if(!rs.isBeforeFirst()){
				return false;
			}
	}
	catch(SQLException se){
		return false;
	}
	queryString = "UPDATE country SET height = height - " + Integer.toString(decrH) + " WHERE cid = " + Integer.toString(cid);
	try{
			ps = connection.prepareStatement(queryString);
			ps.executeUpdate();
			return true;
	}
	catch(SQLException se){
		return false;
	}
  }
    
  public boolean updateDB(){
	String dropString = "DROP TABLE IF EXISTS mostPopulousCountries;";
	String buildString = "CREATE TABLE mostPopulousCountries ( cid INTEGER PRIMARY KEY, cname VARCHAR(20) NOT NULL);";
	String queryString = "INSERT INTO mostpopulousCountries (SELECT cid, cname FROM country WHERE population > 100000000);";
	try{
		PreparedStatement dropStatement = connection.prepareStatement(dropString);
		ps = connection.prepareStatement(buildString);
		ps2 = connection.prepareStatement(queryString);
		dropStatement.executeUpdate();
		ps.executeUpdate();
		ps2.executeUpdate();
		return true;
	}
	catch(SQLException se){
		return false;
	}    
  }
  
}
