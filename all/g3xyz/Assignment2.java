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
  
  String ocean_info, oname, lname, sqlText;
  int odepth, lid, population;
  
  //CONSTRUCTOR
  Assignment2(){
	  try {
		  Class.forName("org.postgresql.Driver");
	  }	catch (ClassNotFoundException e) {
			System.out.println("Failed to find the JDBC driver");
	  }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is successful
  public boolean connectDB(String URL, String username, String password){
	  try {
		  connection = DriverManager.getConnection(URL, username, password);
	  }	catch (SQLException e) {
		  System.out.println("Connection Failed!");
		  return false;
	  }
      return true;
  }
  
  //Closes the connection. Returns true if closure was successful
  public boolean disconnectDB(){
	  try {
		  connection.close();
	  } catch (SQLException e) {
		  	System.out.println("Disconnection unsuccessful");
		  	return false;
	  }
	  return true;
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
	  try {
	  ps= connection.prepareStatement("INSERT INTO a2.country VALUES(?,?,?,?)");
	  ps.setInt(1,cid);
	  ps.setString(2, name);
	  ps.setInt(3,height);
	  ps.setInt(4, population);
	  ps.executeUpdate();
	  ps.close();
	  } catch (SQLException e) {
		  return false;
	  }
	  return true;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	  int total_count = 0;
	  try {		  
		  
		  sqlText ="CREATE VIEW a2.oid_count AS SELECT oid, COUNT(cid) FROM a2.oceanAccess GROUP BY oid";
		  sql = connection.createStatement();
		  sql.executeUpdate(sqlText);

		  ps = connection.prepareStatement("SELECT * FROM a2.oid_count WHERE oid=?");
		  ps.setInt(1, oid);
		  rs = ps.executeQuery();
		  rs.next(); //getting to current row;
		  total_count = rs.getInt(2);
		  sql.executeUpdate("DROP VIEW IF EXISTS a2.oid_count CASCADE"); //dropping view
		  
		  //closing connections
		  sql.close();
		  rs.close();
		  ps.close();
	  }	catch (SQLException e) {
			return -1;  
	  }
	  return total_count;
  }
   
  public String getOceanInfo(int oid){
	  try {
		  ps = connection.prepareStatement("SELECT * FROM a2.ocean WHERE oid=?");
		  ps.setInt(1, oid);
		  rs = ps.executeQuery();
		  rs.next(); //getting to first row
		  if (rs.getInt(1) != oid) {
			  return "";
		  }
		  oname = rs.getString(2);
		  odepth = rs.getInt(3);
		  ocean_info = String.format("%d" + ":" + "%s" + ":" + "%d",oid,oname,odepth);
		  rs.close();
		  ps.close();
	  } catch (SQLException e) {
		  return "";
	  }
	  return ocean_info;
  }

  public boolean chgHDI(int cid, int year, double newHDI){
	  try {
		  ps = connection.prepareStatement("UPDATE a2.hdi SET hdi_score=? WHERE cid=? AND year=?");
		  ps.setDouble(1, newHDI);
		  ps.setInt(2,cid);
		  ps.setInt(3, year);
		  ps.executeUpdate();
		  ps.close();
	  } catch (SQLException e) {
		  return false;
	  }
	  return true;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	  try {
		  ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE country=? AND neighbor=? OR country=? AND neighbor=?");
		  ps.setInt(1,c1id);
		  ps.setInt(2,c2id);
		  ps.setInt(3,c2id);
		  ps.setInt(4,c1id);
		  ps.executeUpdate();
		  ps.close();
	  } catch (SQLException e) {
		  return false;
	  }
	  return true;
  }
  
  public String listCountryLanguages(int cid){
	  StringBuilder languages_spoken = new StringBuilder();
	  int last_index = 0;
	  try {
		  sql = connection.createStatement();
		  sqlText = "SET search_path TO a2"; 
		  sql.executeUpdate(sqlText);
	
		  //creating a view of languages spoken by cid
		  sqlText = String.format("CREATE VIEW languages_spoken AS " +
		  							"SELECT * FROM language WHERE cid=%d",cid);
	
		  sql.executeUpdate(sqlText);
		
		  //creating a view of population of cid
		  sqlText = String.format("CREATE VIEW country_population AS " +
		  							"SELECT * FROM country WHERE cid=%d", cid);
		  sql.executeUpdate(sqlText);
	
		  sqlText = "CREATE VIEW temp_table AS " +
		  				"SELECT * FROM languages_spoken NATURAL JOIN country_population";
		  sql.executeUpdate(sqlText);
	
		  sqlText = "SELECT lid, lname, population*lpercentage AS population " +
		  			"FROM temp_table ORDER BY population";
			
		  rs = sql.executeQuery(sqlText);
		  while (rs.next()) {
			  lid = rs.getInt(1);
			  lname  = rs.getString(2);
			  population = rs.getInt(3);
			  languages_spoken.append(lid + ":" + lname + ":" + population + "#");
		  }
		  //Dropping the views
		  sql.executeUpdate("DROP VIEW IF EXISTS languages_spoken CASCADE");
		  sql.executeUpdate("DROP VIEW IF EXISTS country_population CASCADE");
		  sql.executeUpdate("DROP VIEW IF EXISTS temp_table CASCADE");
		  
		  //Closing the connections
		  rs.close();
		  sql.close();
		  last_index = languages_spoken.toString().length() -1;
		  languages_spoken.deleteCharAt(last_index);
	  } catch (SQLException e) {
		  return "";
	  }
	  return languages_spoken.toString();
  }
  
  public boolean updateHeight(int cid, int decrH){
	  try {
	  //getting the current height of the cid
	  ps = connection.prepareStatement("SELECT height FROM country WHERE cid=?");
	  ps.setInt(1,cid);
	  rs = ps.executeQuery();
	  rs.next();
	  int new_height = rs.getInt(1) - decrH;//new updated height
	  ps = connection.prepareStatement("UPDATE a2.country SET height=? WHERE cid=?");
	  ps.setInt(1, new_height);
	  ps.setInt(2, cid);
	  ps.executeUpdate();
	  rs.close();
	  ps.close();
	  } catch (SQLException e) {
		  return false;
	  }
	  return true;
  }
    
  public boolean updateDB(){
	  try {
		  sql = connection.createStatement();
		  sqlText = "SET search_path TO a2"; 
		  sql.executeUpdate(sqlText);
		  
		  //dropping if any table already existed
		  sql.executeUpdate("DROP TABLE IF EXISTS mostPopulousCountries");

		  // creating new table with population >100 million
		  sql.executeUpdate("CREATE TABLE mostPopulousCountries (" +
		  						"cid INTEGER," +
		  						"cname VARCHAR(20)," +
		  						"PRIMARY KEY(cid))");
		 
		  ps = connection.prepareStatement("SELECT cid, cname FROM country WHERE population>100000000" +
	  						 		"ORDER BY cid ASC");
		  rs = ps.executeQuery();
		  while(rs.next()) {
			  ps = connection.prepareStatement("INSERT INTO mostPopulousCountries VALUES(?,?)");
			  ps.setInt(1, rs.getInt(1));
			  ps.setString(2, rs.getString(2)); 
			  ps.executeUpdate();
		  }
		  sql.close();
		  rs.close();
		  ps.close();
	  } catch (SQLException e) {
		  return false;
	  }
	  return true;
  }
  
}
