import java.sql.*;
import java.util.ArrayList;
import java.util.List;

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
	  }
  }

  private String assembleString(List<List<String>> lst) {
	  String str = "";
	  for (int row = 0; row < lst.size(); row++) {
		  if (row != 0) {
			  str = str + "#";
		  }
		  for (int column = 0; column < lst.get(row).size(); column++) {
			  if (column != 0) {
				  str = str + ":";
			  }
			  str = str + lst.get(row).get(column);
		  }
	  }
	  return str;
  }

  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is successful
  public boolean connectDB(String URL, String username, String password) {
	  try {
		  connection = DriverManager.getConnection(URL, username, password);
	  }
	  catch (SQLException se) {
		  return false;
      }
	  return true;
  }

  //Closes the connection. Returns true if closure was successful
  public boolean disconnectDB(){
	  try {
		  connection.close();
	  }
	  catch (SQLException se) {
		  return false;
      }
	  return true;
  }

  public boolean insertCountry (int cid, String name, int height, int population) {
	  int success = 0;
	  try {
          int count = 0;
		  ps = connection.prepareStatement("SELECT cid FROM a2.country WHERE cid=?");
		  ps.setInt(1, cid);
		  rs = ps.executeQuery();
		  while (rs.next()) {
              count++;
          }
	      rs.close();
	      ps.close();
	      if (count > 0) {
	    	  return false;
	      }
	      ps = connection.prepareStatement("INSERT INTO a2.country (cid, cname, height, population) VALUES (?, ?, ?, ?)");
	      ps.setInt(1, cid);
	      ps.setString(2, name);
	      ps.setInt(3, height);
	      ps.setInt(4, population);
	      success = ps.executeUpdate();
	      ps.close();
	      if (success != 1) {
			  return false;
		  }
	  }
	  catch (SQLException se){
		  return false;
	  }
	  return true;
  }

  public int getCountriesNextToOceanCount(int oid) {
	  int rowCount = -1;
	  try {
		  ps = connection.prepareStatement("SELECT COUNT(cid) FROM a2.oceanAccess WHERE oid=?");
		  ps.setInt(1, oid);
		  rs = ps.executeQuery();
		  while(rs.next()) {
			  rowCount = rs.getInt(1);
		  }
	      rs.close();
	      ps.close();
	  }
	  catch (SQLException se) {
		  return -1;
	  }
	  return rowCount;
  }

  public String getOceanInfo(int oid) {
	  String oceanInfo = "";
	  try {
		  ps = connection.prepareStatement("SELECT oid, oname, depth FROM a2.ocean WHERE oid=?");
		  ps.setInt(1, oid);
		  rs = ps.executeQuery();
		  List<List<String>> rows = new ArrayList<List<String>>();
		  while(rs.next()) {
			  List<String> row = new ArrayList<String>();
			  row.add(Integer.toString(rs.getInt("oid")));
			  row.add(rs.getString("oname"));
			  row.add(Integer.toString(rs.getInt("depth")));
			  rows.add(row);
		  }
	      rs.close();
	      ps.close();
	      oceanInfo = this.assembleString(rows);
	  }
	  catch (SQLException se) {
		  return "";
	  }
   return oceanInfo;
  }

  public boolean chgHDI(int cid, int year, float newHDI) {
	  int success = 0;
	  try {
		  ps = connection.prepareStatement("UPDATE a2.hdi SET hdi_score=? WHERE cid=? AND year=?");
		  ps.setFloat(1, newHDI);
		  ps.setInt(2, cid);
	      ps.setInt(3, year);
	      success = ps.executeUpdate();
	      ps.close();
	      if (success != 1) {
	    	  return false;
	      }
	  }
	  catch (SQLException se) {
		  return false;
	  }
	  return true;
  }

  public boolean deleteNeighbour(int c1id, int c2id) {
	  int success = 0;
	  try {
		  ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE (country=? AND neighbor=?) OR (country=? AND neighbor=?)");
		  ps.setInt(1, c1id);
		  ps.setInt(2, c2id);
	      ps.setInt(3, c2id);
	      ps.setInt(4, c1id);
	      success = ps.executeUpdate();
	      ps.close();
	      if (success != 2) {
	    	  return false;
	      }
	  }
	  catch (SQLException se) {
		  return false;
	  }
	  return true;
  }

  public String listCountryLanguages(int cid) {
	  String ans = "";
	  try {
		  ps = connection.prepareStatement("SELECT L.lid, L.lname, L.lpercentage*C.population AS pop FROM a2.country C, a2.language L WHERE L.cid=C.cid AND L.cid=? ORDER BY L.lpercentage*C.population");
		  ps.setInt(1, cid);
	      rs = ps.executeQuery();
	      List<List<String>> rows = new ArrayList<List<String>>();
	      while (rs.next()) {
	    	  List<String> row = new ArrayList<String>();
			  row.add(Integer.toString(rs.getInt("lid")));
			  row.add(rs.getString("lname"));
			  row.add(Integer.toString(rs.getInt("pop")));
			  rows.add(row);
	      }
	      rs.close();
	      ps.close();
	      ans = this.assembleString(rows);
	  }
	  catch (SQLException se) {
		  return "";
	  }
	  return ans;
  }

  public boolean updateHeight(int cid, int decrH) {
      int success = 0;
	  try {
		  ps = connection.prepareStatement("UPDATE a2.country SET height=height-? WHERE cid=?");
		  ps.setInt(1, decrH);
          ps.setInt(2, cid);
	      success = ps.executeUpdate();
	      ps.close();
	      if (success != 1) {
	    	  return false;
	      }
	  }
	  catch (SQLException se) {
		  return false;
	  }
	  return true;
  }

  public boolean updateDB() {
      int success = 0;
      try {
          ps = connection.prepareStatement("CREATE TABLE a2.mostPopulousCountries (cid INTEGER, cname VARCHAR(20))");
          success = ps.executeUpdate();
          ps.close();
          if (success != 0) {
              return false;
          }
          ps = connection.prepareStatement("INSERT INTO a2.mostPopulousCountries (SELECT cid, cname FROM a2.country WHERE population>100000000) ORDER BY cid ASC");
          success = ps.executeUpdate();
          ps.close();
      }
      catch (SQLException se) {
          return false;
      }
      return true;
  }

}
