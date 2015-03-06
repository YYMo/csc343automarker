import java.sql.*;
import java.io.*;

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
  Assignment2() throws IOException {
	  try {
		Class.forName("org.postgresql.Driver");
	    }
	  catch (ClassNotFoundException e) {
		  System.exit(1);
	  }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password) throws SQLException {
	  try {
 			connection = DriverManager.getConnection(URL, username, password);

 
      if (connection != null) {
		  return true;
      } else {
		  return false;
	  } 
	  		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB() throws SQLException {
	  
	  connection.close();
	  
	  if (connection == null) {
		  return true;
      } else {
		  return false;
	  }    
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) throws SQLException{
      try { 
		sql = connection.createStatement();
		String sqlQuery = "INSERT INTO a2.country(cid, cname, height, population) " +
			 "VALUES (" + cid + "," + "'" + name + "'" + "," + height + "," + population + ");";
		sql.executeUpdate(sqlQuery);
		//System.out.println("Executed");
	      
	} catch (SQLException e) {
		 // System.out.println("Oops");
		  e.printStackTrace();
	} finally {
		sql.close();
	 }
	   
	 if (sql == null) {
		 return false;
	 } else { 
	     return true; 
	 }
  }
  
  public int getCountriesNextToOceanCount(int oid) throws SQLException {
	  try { 
		sql = connection.createStatement();
		//System.out.println("SELECT count(cid) FROM oceanAccess WHERE oid = " + oid + " GROUP BY oid");
		String sqlQuery = "SELECT count(cid) " +
						  "FROM a2.oceanAccess " +
						  "WHERE oid = " + oid +
						  " GROUP BY oid";
		rs = sql.executeQuery(sqlQuery);
		//System.out.println("Executed");
		
		if (rs.next()) {
			return rs.getInt(1);
		}
	      
	} catch (SQLException e) {
		//  System.out.println("Oops");
		  e.printStackTrace();
	} finally {
		sql.close();
		rs.close();
	 }
	  
	return -1;
  }
   
  public String getOceanInfo(int oid) throws SQLException {
	  try {
		   sql = connection.createStatement();
			String sqlQuery;
			sqlQuery = "SELECT * FROM a2.ocean WHERE oid = " + oid;
			rs = sql.executeQuery(sqlQuery);
	  
			if (rs.next()) {
				int oceanID = rs.getInt(1);
				String oceanName = rs.getString(2);
				int d = rs.getInt(3);
			
				return oceanID + ":" + oceanName + ":" + d;
			} 	  
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			rs.close();
	 }
	  return "";
  }

  public boolean chgHDI(int cid, int year, float newHDI) throws SQLException {
	  try {
		  	sql = connection.createStatement();
			String sqlQuery;
			sqlQuery = "UPDATE a2.hdi " +
				"SET hdi_score =" + newHDI +
				" WHERE cid=" + cid + " AND year=" + year;
			sql.executeUpdate(sqlQuery);
   
			if (sql != null) {
				return true;
			} else {
				return false;
			}	  
	 } catch (SQLException e) {
			e.printStackTrace();
			return false;
	} finally {
		sql.close();
	 }
  }

  public boolean deleteNeighbour(int c1id, int c2id) throws SQLException {
	try {
		sql = connection.createStatement();
		String sqlQuery1 = "DELETE FROM a2.neighbour WHERE country=" + c1id + "AND neighbor=" + c2id;
		sql.executeUpdate(sqlQuery1);
		String sqlQuery2 = "DELETE FROM a2.neighbour WHERE country=" + c2id + "AND neighbor=" + c1id;
		sql.executeUpdate(sqlQuery2);
		if (sql == null) {
			return false;
		} else { 
			return true; 
		}
	} catch (SQLException e) {
		//	System.out.println("Oops");
			e.printStackTrace();
			return false;
	} finally {
		sql.close();
	 }
  }
  
  public String listCountryLanguages(int cid) throws SQLException {
	  try {
		 sql = connection.createStatement();
		String sqlQuery = "SELECT lid, lname, (lpercentage * " +
						"(SELECT population FROM a2.country WHERE cid = " + cid +")) as population " +
						"FROM a2.language WHERE cid = " + cid + " ORDER BY population";
	  rs = sql.executeQuery(sqlQuery);
	  String buf = "";
	  
	  if (rs.next()) {
		int languageID = rs.getInt(1);
		String languageName = rs.getString(2);
		int p = rs.getInt(3);
	  
		buf += languageID + ":" + languageName + ":" + p;
	}
	  while (rs.next()) {
		int languageID = rs.getInt(1);
		String languageName = rs.getString(2);
		int p = rs.getInt(3);
		
		
		buf += "#" +languageID + ":" + languageName + ":" + p;
		
		}
		return buf; 
	} catch (SQLException e) {
		e.printStackTrace();
		} finally {
		sql.close();
		rs.close();
	 }

	return "";
  }
  
  public boolean updateHeight(int cid, int decrH) throws SQLException{
	try {
		sql = connection.createStatement();
		String sqlQuery = "UPDATE a2.country " +
				"SET height= height - " + decrH +
				" WHERE cid=" + cid;
			   
		sql.executeUpdate(sqlQuery);
	
		if (sql == null) {
			return false;
		} else {
			return true; 
		}
	} catch (SQLException e) {
		//	System.out.println("Oops");
			e.printStackTrace();
			sql.close();
			return false;
	} finally {
		sql.close();
	 }
  }
 
  public boolean updateDB() throws SQLException {
	  try {
		sql = connection.createStatement();
		
		String sqlQuery = "DROP TABLE IF EXISTS mostPopulousCountries;" + 
							" CREATE TABLE mostPopulousCountries(cid INTEGER, cname VARCHAR(20));" + 
							" INSERT INTO mostPopulousCountries(cid, cname) SELECT cid, cname" +
							" FROM a2.country" +
							" WHERE population > 100000000" +
							" ORDER BY cid ASC";
		sql.executeUpdate(sqlQuery);
		
		if (sql == null) {
			return false;
		} else { 
			return true; 
		}
		
	} catch (SQLException e) {
		//	System.out.println("Oops");
			e.printStackTrace();
			return false;
		} finally {
		sql.close();
	 }
  }
}

