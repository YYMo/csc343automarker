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
	} catch (ClassNotFoundException e) {
	    return;
	}
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try {
          connection = DriverManager.getConnection(URL, username, password);
      } catch (SQLException e) {
          return false;
      }
      return true;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try {
          connection.close();
      } catch (SQLException e) {
          return false;
      }
      return true;
  }
    
    
  public boolean insertCountry (int cid, String name, int height, int population) {
	try {
		String sqlText = "INSERT INTO country(cid, name, height, population)" +
				 " VALUES (?, ?, ?, ?);";
		ps = connection.prepareStatement(sqlText);
		ps.setInt(1, cid);
		ps.setString(2, name);
		ps.setInt(3, height);
		ps.setInt(4, population);
		ps.executeUpdate();
		ps.close();
        } catch (SQLException e) {
		return false;
        }
        return true;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	int result = -1;
	try {
		sql = connection.createStatement();
		String sqlText = "SELECT count(oid) FROM oceanAccess WHERE oid = " + oid + ";";
		rs = sql.executeQuery(sqlText);
		rs.next();
		result = rs.getInt(1);
		rs.close();
		sql.close();
	} catch (SQLException e) {
		return -1;
	}
	return result; 
  }
   
  public String getOceanInfo(int oid){
	String result = "";
	try {
		sql = connection.createStatement();
		String sqlText = "SELECT oid, oname, depth FROM ocean " +
				 "WHERE oid = " + oid + ";";
		rs = sql.executeQuery(sqlText);
		rs.next();
		result = rs.getInt("oid") + ":" + rs.getString("oname") + ":" +
			 rs.getInt("depth");
		rs.close();
		sql.close();
	} catch (SQLException e) {
		return "";
	}
	return result;
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	boolean result = false;
	try {
		sql = connection.createStatement();
		String sqlText = "UPDATE hdi SET hdi_score = " + newHDI + " WHERE " +
				   "cid = " + cid + " and year = " + year + ";";
		sql.executeUpdate(sqlText);
		int count = sql.getUpdateCount();
	if (count != 0) {
		result = true;
	}
	sql.close();
	} catch (SQLException e) {
		return false;
	}
	return result;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	boolean result = false;
	try {
		sql = connection.createStatement();
		String sqlText = "DELETE FROM neighbour WHERE country = " + c1id + " and " +
				 "neighbor = " + c2id + ";";
		sql.executeUpdate(sqlText);
		int count = sql.getUpdateCount();
	if (count != 0) {
		result = true;
	}
	sql.close();
	} catch (SQLException e) {
		return false;
	}	
   	return result;        
  }
  
  public String listCountryLanguages(int cid){
	String result = "";
	try {
		sql = connection.createStatement();
		String sqlText = "SELECT l.lid, l.lname, (l.percentage/100*(SELECT (population " + 					 
				 "FROM country c WHERE c.cid = " + cid + ")) FROM language l, " + 				 	 
				 "country c WHERE l.cid = " + cid + " and c.cid = " + cid + ";";
		rs = sql.executeQuery(sqlText);
		if (rs != null) {
			while (rs.next()) {
				result += rs.getInt(1) + ":" + rs.getString(2) + ":" +
					  rs.getInt(3) + "#";
			}
		}
		rs.close();
		sql.close();
	} catch (SQLException e) {
		return "";
	}
	return result;
  }
  
  public boolean updateHeight(int cid, int decrH){
	boolean result = false;
	try {
		sql = connection.createStatement();
		String sqlText = "UPDATE country SET height = height - " + decrH + " WHERE " +
				 "cid IN (SELECT cid FROM country WHERE cid = " + cid + ";";

		sql.executeUpdate(sqlText);
		int count = sql.getUpdateCount();
		if (count != 0) {
			result = true;
		}
		sql.close();
	} catch (SQLException e) {
		return false;
	}
	return result;
  }
    
  public boolean updateDB(){
	boolean result = false;
	try {
		sql = connection.createStatement();
		String sqlText = "CREATE TABLE IF NOT EXISTS mostPopulousCountries(" + 
				 "cid INTEGER NOT NULL, cname VARCHAR(20))";
		sql.executeUpdate(sqlText);

		String sqlText2 = "SELECT cid, cname FROM country, WHERE population" + 
				  " > 100000000 ORDER BY cid;";
		rs = sql.executeQuery(sqlText2);

		while (rs.next()) {
			String sqlText3 = "INSERT INTO mostPopulousCountries VALUES (?, ?);";
			ps = connection.prepareStatement(sqlText3);
			ps.setInt(1, rs.getInt(1));
			ps.setString(2, rs.getString(2));
			ps.executeUpdate();
		}
	
	int count = sql.getUpdateCount();
	if (count != 0) {
		result = true;
	}
	rs.close();
	ps.close();
	sql.close();
	} catch (SQLException e) {
		return false;
	} 
	return result;  
  }
  
}
