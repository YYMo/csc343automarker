import java.sql.*;
import java.io.*;

public class Assignment2 {
    
  // A connection to the database  
  Connection connection;
  
  // Statement to run queries
  String sql;
  
  // Prepared Statement
  PreparedStatement ps;
  
  // Resultset for the query
  ResultSet rs;
  
  /* Identifies the postgreSQL driver using Class.forName method. */
  Assignment2() {
	 try {
		 Class.forName("org.postgresql.Driver");
	 } catch (ClassNotFoundException e) {
		 System.out.println("Failed to find the JDBC driver");
	 }
  }
    
  /* Returns true iff the database connection was established. */ 
  public boolean connectDB(String URL, String username, String password){
      try {
    	  connection = DriverManager.getConnection(URL, username, password);
	  sql = "SET SEARCH_PATH TO a2";
	  ps = connection.prepareStatement(sql);
	  ps.executeUpdate();
	  return true;
      } catch (SQLException se) {
    	  return false; 	  
      }
  }
  
  /* Returns true iff connection closure was sucessful. */
  public boolean disconnectDB(){
      try {
    	  if (connection != null) {
    		  connection.close();
    		  return true;
    	  } else {
    		  return false;
    	  }
      } catch (SQLException se) {
    	  return false;
      }
  }
  
 /* Returns true iff the new country was inserted successfully given the cid,
  * height of the highest elevation point, and population. */   
  public boolean insertCountry (int cid, String name, int height, int population) {
	  try {
	          // Check to see if the country is already in the table.
		  sql = "SELECT cname FROM country WHERE cid = ?";
		  ps = connection.prepareStatement(sql);
		  ps.setInt(1, cid);
		  
		  rs = ps.executeQuery();
	  
		  while (rs.next()) {
		       return false;
		  }
		  
		  sql = "INSERT INTO country VALUES (?, ?, ?, ?)";
		  ps = connection.prepareStatement(sql);
		  ps.setInt(1, cid);
		  ps.setString(2, name);
		  ps.setInt(3, height);
		  ps.setInt(4, population);
		  ps.executeUpdate();
		  return true;
	  } catch (SQLException se) {
	      return false;
	  }	
  }
  
  /* Returns the number of countries in table oceanAccess that have access to
   * the ocean with oid. */  
  public int getCountriesNextToOceanCount(int oid) {
	  try {
		  sql = "SELECT count(cid) AS numOfCountries FROM oceanAccess" + 
		        " oa WHERE oa.oid = ?";
		  ps = connection.prepareStatement(sql);
		  ps.setInt(1, oid);
		  rs = ps.executeQuery();
		  
		  while (rs.next()) {
			  return rs.getInt("numofCountries");
		  }
	  } catch (SQLException se) {
		  return -1;
	  }
	  return -1;
  }

  /* Returns the string with information about the ocean with the given oid in 
   * the format "oid:oname:depth". */  
  public String getOceanInfo(int oid){
          int theoid = 0;
          String theoname = "";
	  int thedepth = 0;
	  try {
		  sql = "SELECT * FROM ocean o WHERE o.oid = ?";
		  ps = connection.prepareStatement(sql);
		  ps.setInt(1, oid);
		  rs = ps.executeQuery();
		  
		  while (rs.next()) {
			  theoid = rs.getInt("oid");
			  theoname = rs.getString("oname");
			  thedepth = rs.getInt("depth");
			  break;
		  }
		  if (theoname.equals("")) {
			  return "";
		  } else {
			  return theoid + ":" + theoname + ":" + thedepth;
		  }
	  } catch (SQLException se) {
		  
	  }
	  return "";
  }

  /* Returns true iff the HDI of a country with the given cid in the given year
   *  was successfully updated to newHDI. */
  public boolean chgHDI(int cid, int year, float newHDI){
      try {
	  sql = "UPDATE hdi SET hdi_score = ? WHERE cid = ? AND year = ?";
	  ps = connection.prepareStatement(sql);
	  ps.setFloat(1, newHDI);
	  ps.setInt(2, cid);
	  ps.setInt(3, year);
	  int updatedHDI = ps.executeUpdate();
	  if (updatedHDI == 1) {
	      return true;
	  } else { 
	      return false;
	  }
      } catch (SQLException se) {
	  return false;
      }
	  
  }

  /* Returns true iff the neighbouring relation between countries with cids c1id
   *  and c2id was deleted successfully. */
  public boolean deleteNeighbour(int c1id, int c2id){
      try {
	   sql = "DELETE FROM neighbour WHERE (country = ? AND neighbor = ?) " + 
	         "OR (country = ? AND neighbor = ?)";
	   ps = connection.prepareStatement(sql);
	   ps.setInt(1, c1id);
	   ps.setInt(2, c2id);
	   ps.setInt(3, c2id);
	   ps.setInt(4, c1id);
	   int rowsDeleted = ps.executeUpdate();
	   if (rowsDeleted == 2) {
		   return true;
	   } else {
		   return false;
	   }
      } catch (SQLException se) {
	   return false;
      }
   }
  
  /* Returns a string with languages spoken in the country given the cid of 
   * that country. */
  public String listCountryLanguages(int cid){
	  try {
		  sql = "SELECT lid, lname, population * (lpercentage/100) AS " +
		  		"pop FROM language l JOIN country c ON l.cid " + 
		                "= c.cid WHERE l.cid = ? GROUP BY lid, lname, " +
		  		"pop ORDER BY pop";
		  ps = connection.prepareStatement(sql);
		  ps.setInt(1, cid);
		  rs = ps.executeQuery();
		  
		  String stringrepr = "";
		  while (rs.next()) {
			  int lid = rs.getInt("lid");
			  String lname = rs.getString("lname");
			  int population = rs.getInt("pop");
			  stringrepr = stringrepr + lid + ":" + lname + ":" + 
			      population + "#";
		  }
		  if (stringrepr.equals("")) {
			  return stringrepr;
		  } else {
			  return stringrepr.substring(0, stringrepr.length() - 1);
		  }
	  } catch (SQLException se) {
		  return "";
	  }
  }

  /* Returns true iff the height of the country with the given cid was decreased
   * by decrH. */  
  public boolean updateHeight(int cid, int decrH){
    try {
    	sql = "UPDATE country SET height = height - ? WHERE country.cid = ?";
    	ps = connection.prepareStatement(sql);
	ps.setInt(1, decrH);
    	ps.setInt(2, cid);
    	int heightUpdated = ps.executeUpdate();
    	if (heightUpdated == 1) {
	    return true;
	} else {
	    return false;
	}
    } catch (SQLException se) {
    	return false;
    }
  }
    
  /* Return true iff the database was successfully updated with a new table 
   * mostPopulousCountries. */
  public boolean updateDB(){
	try {
	  sql = "CREATE TABLE mostPopulousCountries(cid INTEGER, " +
	  		"cname VARCHAR(20))";
	  ps = connection.prepareStatement(sql);
	  ps.executeUpdate();
	  
	  sql = "INSERT INTO mostPopulousCountries (SELECT cid FROM " +
	  		"country WHERE population > 100000000 ORDER BY cid)";
	  ps = connection.prepareStatement(sql);
	  ps.executeUpdate();

	  sql = "ALTER TABLE mostPopulousCountries SET SCHEMA a2";
	  ps = connection.prepareStatement(sql);
	  ps.executeUpdate();

	  return true;
	} catch (SQLException se) {
	  return false;
	}
  }

}