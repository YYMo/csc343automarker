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
  
  //Class load fail
  boolean classloaded = false;
  
  //0 CONSTRUCTOR
  Assignment2() {
	  try {
		Class.forName("org.postgresql.Driver");
		classloaded = true;
	} catch (ClassNotFoundException e) { /* ignored */ }
  }
  
  //1 Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password) {
	  if (!classloaded) {
		  return false;
	  }
	  try {
		  connection = DriverManager.getConnection(URL, username, password);
	  } catch (SQLException e) {
		  return false;
	  }
      return true;
  }
  
  //2 Closes the connection. Returns true if closure was successful
  public boolean disconnectDB() {
	  
	  if (rs != null) {
	        try {
	            rs.close();
	        } catch (SQLException e) { /* ignored */ }
	    }
	    if (ps != null) {
	        try {
	            ps.close();
	        } catch (SQLException e) { /* ignored */ }
	    }
	    if (connection != null) {
	        try {
	            connection.close();
	        } catch (SQLException e) { 
	  		  return false;
	  		  }
	        }
      return true;
  }
  
  //3
  public boolean insertCountry (int cid, String name, int height, int population) {
	  int result = -1;
	  try {
		  ps = connection.prepareStatement("INSERT INTO A2.country VALUES (?, ?, ?, ?)");

		  ps.setInt(1, cid);
		  ps.setString(2, name);
		  ps.setInt(3, height);
		  ps.setInt(4, population);
		  result = ps.executeUpdate();
		  ps.close();
	  } catch (SQLException e) { /* ignored */ }
	  if (result == 1) {
		  return true;	
	  }
	  else {
		  return false;
	  }
  }
  
  //4
  public int getCountriesNextToOceanCount(int oid) {
	  int result = -1;
	  try {
		  ps = connection.prepareStatement("SELECT COUNT(*) FROM A2.oceanAccess WHERE oid = ?");
		  ps.setInt(1, oid);
		  rs = ps.executeQuery();
		  if (rs.next()) {
			result = rs.getInt("count");
		  }
		  rs.close();
		  ps.close();
	  } catch (SQLException e) {
		  return result;
	  }
	  return result;
  }
  
  //5
  public String getOceanInfo(int oid) {
	  String result = "";
	  String oid_r = "", oname_r = "", depth_r = "";
	  try {
		  ps = connection.prepareStatement("SELECT oid, oname, depth FROM A2.ocean WHERE oid = ?");
		  ps.setInt(1, oid);
		  rs = ps.executeQuery();
		  if (rs.next()) {
				oid_r = Integer.toString(rs.getInt("oid"));
				oname_r = rs.getString("oname");
				depth_r = Integer.toString(rs.getInt("depth"));
				result = oid_r+":"+oname_r+":"+depth_r;
		  }
		  rs.close();
		  ps.close();
	  } catch (SQLException e) {
		  return result;
	  }
	  return result;
  }
  
  //6
  public boolean chgHDI(int cid, int year, float newHDI) {
	  
	  int result = -1;
	  try {
		  ps = connection.prepareStatement("UPDATE A2.hdi SET hdi_score = ? WHERE cid =? AND year= ?");
		  ps.setFloat(1, newHDI);
		  ps.setInt(2, cid);
		  ps.setInt(3, year);
		  result = ps.executeUpdate();
		  ps.close();
	  } catch (SQLException e) { /* ignored */ }
	  if (result == 1) {
		  return true;	
	  }
	  else {
		  return false;
	  }
  }
  
  //7
  public boolean deleteNeighbour(int c1id, int c2id) {
	  
	  int result = -1;
	  int result2 = -1;
	  try {
		  ps = connection.prepareStatement("DELETE FROM A2.neighbour WHERE country =? AND neighbor= ?");
		  ps.setInt(1, c1id);
		  ps.setInt(2, c2id);
		  result = ps.executeUpdate();
		  ps.close();
		  ps = connection.prepareStatement("DELETE FROM A2.neighbour WHERE country =? AND neighbor= ?");
		  ps.setInt(1, c2id);
		  ps.setInt(2, c1id);
		  result2 = ps.executeUpdate();
		  ps.close();
	  } catch (SQLException e) { /* ignored */ }
	  if (result == 1 && result2 == 1) {
		  return true;
	  }
	  else {
		  return false;
	  }
  }
  
  //8
  public String listCountryLanguages(int cid) {
	  String interm = "", result = "";
	  String lid_r = "", lname_r = "", lpercentage_r = "";
	  try {
		  ps = connection.prepareStatement("SELECT lid, lname, (l.lpercentage * c.population) AS lpopulation FROM A2.language l, A2.country c WHERE l.cid =? AND c.cid =? ORDER BY lpopulation");
		  ps.setInt(1, cid);
		  ps.setInt(2, cid);
		  rs = ps.executeQuery();
		  while (rs.next()) {
				lid_r = Integer.toString(rs.getInt("lid"));
				lname_r = rs.getString("lname");
				lpercentage_r = Float.toString(rs.getFloat("lpopulation"));
				interm= lid_r+":"+lname_r+":"+lpercentage_r+"#";
				result = result + interm;
		  }
		  result = result.substring(0, result.length()-1);
		  rs.close();
		  ps.close();
	  } catch (Exception e) {
		  return result;
	  }
	  return result;
  }
  
  //9
  public boolean updateHeight(int cid, int decrH) {
	  int result = -1;
	  try {
		  ps = connection.prepareStatement("UPDATE A2.country SET height = height - ? WHERE cid =?");
		  ps.setInt(1, decrH);
		  ps.setInt(2, cid);
		  result = ps.executeUpdate();
		  ps.close();
	  } catch (SQLException e) { /* ignored */ }
	  if (result == 1) {
		  return true;	
	  }
	  else {
		  return false;
	  }
  }
  
  //10
  public boolean updateDB() {
	  int result = -1;
	  int result2 = -1;
	  try {
		  ps = connection.prepareStatement("CREATE TABLE A2.mostPopulousCountries (cid INTEGER, cname VARCHAR(20) )");
		  result = ps.executeUpdate();
		  ps.close();
		  ps = connection.prepareStatement("INSERT INTO A2.mostPopulousCountries (SELECT cid, cname FROM A2.country WHERE population > 1e6 ORDER BY cid ASC)");
		  result2 = ps.executeUpdate();
		  ps.close();
	  } catch (SQLException e) { /* ignored */ }
	  if (result == 0 && result2 > -1) {
		  return true;	
	  }
	  else {
		  return false;
	  }
  }
}
