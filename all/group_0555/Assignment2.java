import java.sql.*;

public class Assignment2 {
  // A connection to the database  
  Connection connection;
  
  // Statement to run queries
  Statement sql;
  
  // Prepared Statement
  PreparedStatement ps;
  
  // Result set for the query
  ResultSet rs;
  
  //CONSTRUCTOR
  Assignment2() throws ClassNotFoundException{
	  Class.forName("org.postgresql.Driver");
  }
  
  /**
   * Using the input parameters, establish a connection to be used for this session. 
   * Returns true if connection is successful
   * @param URL
   * @param username
   * @param password
   * @return
   */
  public boolean connectDB(String URL, String username, String password){
	  try {
		  connection = DriverManager.getConnection(URL, username, password);
		  sql = connection.createStatement();
		  sql.execute("set search_path to a2;");
		  sql.close();
		  return true;
	  } catch (Exception e) {
		 // System.out.println("Error while connecting to database: " + e);
	  }
      return false;
  }
  
  /**
   * Closes the connection. Returns true if closure was successful
   * @return true if closure was successful
   */
  public boolean disconnectDB(){
		 try {
			connection.close();
			return true;
		} catch (Exception e) {
			return false;    
		}
  }
    
 
  public boolean insertCountry (int cid, String name, int height, int population) {
	  if(connection == null) {
		  return false;
	  }
	  try {
		ps = connection.prepareStatement("INSERT INTO country VALUES (?, ?, ?, ?);");
		ps.setInt(1, cid);
		ps.setString(2, name);
		ps.setInt(3, height);
		ps.setInt(4, population);
		ps.execute();
		ps.close();
		return true;
	} catch (SQLException e) {
		return false;
	}
  }
  
  
  public int getCountriesNextToOceanCount(int oid) {
	  if(connection == null) {
		  return -1;
	  }
	  try {
		PreparedStatement prep = connection.prepareStatement("SELECT COUNT(*) FROM oceanaccess WHERE oid = ?;");
		prep.setInt(1, oid);
		rs = prep.executeQuery();
		rs.next();
		int rVal = rs.getInt(1);
		rs.close();
		return rVal;
	} catch (SQLException e) {
		return -1;
	}
  }
   
  
  public String getOceanInfo(int oid){
	  if(connection == null) {
		  return "";
	  }
	  try {
		  ps = connection.prepareStatement("SELECT * FROM ocean WHERE oid = ?;");
		  ps.setInt(1, oid);
		  rs = ps.executeQuery();
		  String res = "";
		  if(rs.next()) {
			  StringBuilder sb = new StringBuilder();
			  sb.append(rs.getInt("oid"));
			  sb.append(":");
			  sb.append(rs.getString("oname").trim());
			  sb.append(":");
			  sb.append(rs.getInt("depth"));
			  res = sb.toString();
		  }
		  ps.close();
		  rs.close();
		  return res;
	} catch (SQLException e) {
		return "";
	}
  }

  
  public boolean chgHDI(int cid, int year, float newHDI){
	  if(connection == null) {
		  return false;
	  }
	  try {
		  ps = connection.prepareStatement("UPDATE hdi SET hdi_score = ? WHERE cid = ? AND year = ?;");
		  ps.setFloat(1, newHDI);
		  ps.setInt(2, cid);
		  ps.setInt(3, year);
		  int rowCount = ps.executeUpdate();
		  ps.close();
		  if(rowCount != 0)
			  return true;
	} catch (SQLException e) {
	}
	return false;
  }
  

  public boolean deleteNeighbour(int c1id, int c2id){
	  if(connection == null) {
		  return false;
	  }
	  try {
			ps = connection.prepareStatement("DELETE FROM neighbour " +
					"WHERE (country = ? AND neighbor = ?) OR (country = ? AND neighbor = ?);");
			ps.setInt(1, c1id);
			ps.setInt(2, c2id);
			ps.setInt(3, c2id);
			ps.setInt(4, c1id);
			int rowCount = ps.executeUpdate();
			ps.close();
			if(rowCount != 0)
				return true;
		} catch (SQLException e) {
		}      
		return false;
  }
  
  
  public String listCountryLanguages(int cid){
	  if(connection == null) {
		  return "";
	  }
	  try {
		  ps = connection.prepareStatement("SELECT l.lid as lid, l.lname as lname, l.lpercentage as perc, c.population as pop " +
				"FROM language as l, country as c  WHERE l.cid = ? AND c.cid = ? ORDER BY pop;");
		  ps.setInt(1, cid);
		  ps.setInt(2, cid);
		  rs = ps.executeQuery();
		  String res = "";
		  StringBuilder sb = new StringBuilder();
		  while(rs.next()) {
			  sb.append(rs.getInt("lid"));
			  sb.append(":");
			  sb.append(rs.getString("lname").trim());
			  sb.append(":");
			  float perc = rs.getFloat("perc");
			  int pop = rs.getInt("pop");
			  float people = perc * ((float)pop);
			  sb.append(people);
			  sb.append("#");
		  }
		  if(sb.length() != 0 ) {
			  sb.deleteCharAt(sb.lastIndexOf("#"));
		  }
		  ps.close();
		  rs.close();
		  return sb.toString();
	  } catch (SQLException e) {
		  return "";
	  }
  }
  
  
  public boolean updateHeight(int cid, int decrH){
	  if(connection == null) {
		  return false;
	  }
	  try {
		  ps = connection.prepareStatement("UPDATE country SET height = height - ? WHERE cid = ?;");
		  ps.setInt(1, decrH);
		  ps.setInt(2, cid);
		  int rowCount = ps.executeUpdate();
		  ps.close();
		  if(rowCount != 0)
			  return true;
	  } catch (SQLException e) {
	}
    return false;
  }
    
  public boolean updateDB(){
	  if(connection == null) {
		  return false;
	  }
	  try {
		  sql = connection.createStatement();
		  sql.execute("DROP TABLE IF EXISTS mostPopulousCountries;");
		  sql.execute("CREATE TABLE mostPopulousCountries ( " +
		  		"cid INTEGER NOT NULL, " +
		  		"cname VARCHAR(20) NOT NULL, " +
		  		"PRIMARY KEY(cid) " +
		  		")");
		  sql.execute("INSERT INTO mostPopulousCountries " +
		  		"(SELECT cid, cname FROM country WHERE population > 100000000 ORDER BY cid ASC);");
		  sql.close();
		  return true;
		  
	} catch (SQLException e) {
	}
	return false;    
  }
  
}