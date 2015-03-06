import java.sql.*;

public class Assignment2 {
  
  // A connection to the database  
  Connection connection;
  
  // Statement to run queries
  Statement sql = null;
  
  // Prepared Statement
  PreparedStatement ps = null;
  
  // Resultset for the query
  ResultSet rs = null;
  
  //CONSTRUCTOR
  Assignment2(){//works
    try {
      Class.forName("org.postgresql.Driver");
    }
    catch (ClassNotFoundException e) {
      
    }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){//works
    try {
      connection = DriverManager.getConnection(URL, username, password);
    }
    catch (SQLException e) {
      return false;
    }
    return connection != null;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){//works
    try {
      connection.close();
      return connection.isClosed();//True iff the connection is closed
    }
    catch (SQLException e) {
      return false;
    }
  }
  
  public boolean insertCountry (int cid, String name, int height, int population) {//works
    try {
      //First check if its in the table
      sql = connection.createStatement();
      rs = sql.executeQuery("SELECT COUNT(*) FROM a2.country WHERE cid=" + cid);
      rs.next();
      int result = rs.getInt(1);
      rs.close();
      sql.close();
	    
      if (result != 1) {
        ps = connection.prepareStatement("INSERT INTO a2.country (cid, cname, height, population) VALUES (?, ?, ?, ?)");
        ps.setInt(1, cid);
        ps.setString(2, name);
        ps.setInt(3, height);
        ps.setInt(4, population);
        ps.executeUpdate();
        ps.close();
        return true;
      }
      return false;
    }
    catch (SQLException e) {
      return false;
    }
  }
   
  public int getCountriesNextToOceanCount(int oid) {//works
    try {
      sql = connection.createStatement();
      rs = sql.executeQuery("SELECT COUNT(*) FROM (SELECT DISTINCT cid FROM a2.oceanAccess WHERE oid=" + oid + ") as A");
      rs.next();
      int result = rs.getInt(1);
      rs.close();
      sql.close();
      return result;
    }
    catch (SQLException e) {
      return -1;
    }
  }
  
  public String getOceanInfo(int oid){//Works
    try {
      sql = connection.createStatement();
      rs = sql.executeQuery("SELECT * FROM a2.ocean WHERE oid=" + oid);
      String result = "";
	    rs.next();
      result += rs.getInt(1) + ":" + rs.getString(2) + ":" + rs.getInt(3);//think this works
      rs.close();
      sql.close();
      if (result.equals("::")) {
        return "";
      }
      return result;
    }
    catch (SQLException e) {
      return "";
    }
  }
  
  public boolean chgHDI(int cid, int year, float newHDI){//Works
    try {
      boolean result = false;
      
      sql = connection.createStatement();
      sql.executeUpdate("UPDATE a2.hdi SET hdi_score = " + newHDI + " WHERE cid=" + cid + " and year=" + year);
      
      if (sql.getUpdateCount() == 1) {
        result = true;
      }
      sql.close();
      
      return result;
    }
    catch (SQLException e) {
      return false;
    }
  }
  
  public boolean deleteNeighbour(int c1id, int c2id){//Works
    try {
      boolean result = false;
      sql = connection.createStatement();
      sql.executeUpdate("DELETE FROM a2.neighbour WHERE (country=" + c1id + " and neighbor=" + c2id + ") or (country=" + c2id + " and neighbor=" + c1id + ")");
      if (sql.getUpdateCount() == 2) {
        result = true;
      }
      
      sql.close();
      return result;
    }
    
    catch (SQLException e) {
      return false;
    }
    
  }
  
  public String listCountryLanguages(int cid){//Works
    try {
      String result = "";
      
      sql = connection.createStatement();
      rs = sql.executeQuery("SELECT lid, lname, (lpercentage * population) as total FROM a2.country JOIN a2.language ON a2.country.cid=a2.language.cid WHERE a2.country.cid=" + cid);
      while(rs.next()) {
        result += rs.getInt(1) + ":" + rs.getString(2) + ":" + rs.getFloat(3) + "#";
      }
      
      //Remove the last pound sign
      result = result.substring(0, result.length()-1);
      //remove the white space
      result = result.trim();
      
      rs.close();
      sql.close();
      
      return result;
    }
    
    catch (SQLException e) {
      return "";
    }
    
  }

  public boolean updateHeight(int cid, int decrH){//Works
    try {
      sql = connection.createStatement();
      rs = sql.executeQuery("SELECT height FROM a2.country WHERE cid=" + cid);
      rs.next();
      int height = rs.getInt(1);
      
      height = height - decrH;
      
      rs.close();
      sql.close();
      
      sql = connection.createStatement();
      sql.executeUpdate("UPDATE a2.country SET height=" + height + " WHERE cid=" + cid);
      
      boolean result = sql.getUpdateCount() == 1;
      sql.close();
      
      return result;
    }
    catch (SQLException e) {
      return false;
    }
  }
  
  public boolean updateDB(){//WOrks
    try {
      sql = connection.createStatement();
      sql.executeUpdate("CREATE TABLE a2.mostPopulousCountries (cid INTEGER PRIMARY KEY, cname VARCHAR(20) NOT NULL)");
      
      sql.close();
      
      sql = connection.createStatement();
      rs = sql.executeQuery("SELECT cid, cname FROM a2.country ORDER BY cid ASC");
      
      while (rs.next()) {
        ps = connection.prepareStatement("INSERT INTO a2.mostPopulousCountries (cid, cname) VALUES (?, ?)");
        ps.setInt(1, rs.getInt(1));
        ps.setString(2, rs.getString(2));
        ps.executeUpdate();
      }
      ps.close();
      
      return true;
    }
    catch(SQLException e) {
      return false;
    } 
  }
}