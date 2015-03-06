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
  Assignment2() throws ClassNotFoundException {
    try{
      Class.forName("org.postgresql.Driver").newInstance();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password) throws SQLException {
      try {
	  connection = DriverManager.getConnection(URL, username, password);
	  return true;
      }
      catch (SQLException se) {
           return false;
      }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB() throws SQLException {
      try{
	  connection.close();
	  return true;
      }
      catch (SQLException se) {
           return false;
      }
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
    try {
	sql = connection.createStatement();
	sql.executeUpdate("INSERT INTO country VALUES ("+cid+","+name+","+height+","+population+")");
	return true;
    }
    catch (SQLException se) {
	return false;
    }
  }
  
  public int getCountriesNextToOceanCount(int oid) throws SQLException {
      try {
	  sql = connection.createStatement();
	  String str = Integer.toString(oid);
	  int num = sql.executeUpdate("SELECT cid FROM oceanaccess WHERE oid="+str);
	  return num;
      }
      catch (SQLException se) {
	  return -1;
      }
  }
   
  public String getOceanInfo(int oid) throws SQLException {
   try {
    sql = connection.createStatement();
    String str = Integer.toString(oid);
    rs = sql.executeQuery("SELECT * FROM ocean WHERE oid="+str);
    rs.next();
    String oceanid = rs.getString("oid");
    String oname = rs.getString("oname");
    String depth = rs.getString("depth");
    String result = (oceanid+":"+oname+":"+depth);
    return result;
   }
   catch (SQLException se) {
      return "";
   }
  }

  public boolean chgHDI(int cid, int year, float newHDI) throws SQLException {
   try {
	sql = connection.createStatement();
	String nhdi = Float.toString(newHDI);
	String yr = Integer.toString(year);
	String countryid = Integer.toString(cid);
	sql.executeUpdate("UPDATE hdi SET hdi_score="+nhdi+" WHERE year="+yr+" AND cid="+countryid);
	return true;
    }
    catch (SQLException se) {
	return false;
    }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
   try {
	sql = connection.createStatement();
	String cid1 = Integer.toString(c1id);
	String cid2 = Integer.toString(c2id);
	sql.executeUpdate("DELETE FROM neighbour WHERE country="+cid1+" AND neighbor="+cid2);
	return true;
    }
    catch (SQLException se) {
	return false;
    }       
  }
  
  public String listCountryLanguages(int cid){
    try {
      String result = "";
      sql = connection.createStatement();
      String str = Integer.toString(cid);
      rs = sql.executeQuery("select l.lid,c.cname as lname,(l.lpercentage * c.population) as population from language l,country c where c.cid="+str+" AND c.cid=l.cid ORDER BY population");
      while (rs.next()) {
	String lid = rs.getString("lid");
	String lname = rs.getString("lname");
	String population = rs.getString("population");
	result += (lid+":"+lname+":"+population+"#");
      }
      return result;
   }
   catch (SQLException se) {
      return "";
   }
  }
  
  public boolean updateHeight(int cid, int decrH){
    try {
	sql = connection.createStatement();
	String cid1 = Integer.toString(cid);
	String decr = Integer.toString(decrH);
	sql.executeUpdate("UPDATE country SET height=(height-"+decr+") WHERE cid="+cid1);
	return true;
    }
    catch (SQLException se) {
	return false;
    }  
  }
    
  public boolean updateDB(){
      try {
	sql = connection.createStatement();
	sql.executeUpdate("CREATE TABLE mostPopulousCountries (cid in,cname varchar(20))");
	sql.executeUpdate("INSERT INTO mostPopulousCountries (SELECT cid,cname FROM country WHERE population>100000000 ORDER BY cid)");
	return true;
    }
    catch (SQLException se) {
	return false;
    }
  }
  /*
  public static void main(String args[]) throws IOException {
    try {
      Assignment2 a2 = new Assignment2();
      a2.connectDB("jdbc:postgresql://localhost:5432/csc343h-c3isakov", "c3isakov", "");
      a2.insertCountry(540,"Maximistan",100,1);
      a2.insertCountry(36,"Maximistan",100,1);
    }
    catch (Exception e) {
      
    }
  }
  */
}
