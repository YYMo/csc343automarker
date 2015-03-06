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
      e.printStackTrace();
    }
  }

  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
    try {
      connection = DriverManager.getConnection(
        "jdbc:postgresql://" + URL ,username, password);
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }

    return true;
  }

  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
    try {
      connection.close();
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
    return true;    
  }

  public boolean insertCountry (int cid, String name, int height, int population) {
    try {
      ps = connection.prepareStatement("INSERT INTO country (cid, cname, height, population) "
        + "VALUES (?, ?, ?, ?);");
      ps.setInt(1, cid);
      ps.setString(2, name);
      ps.setInt(3, height);
      ps.setInt(4, population);
      ps.execute();
      ps.close();
      ps = null;
      return true;
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
  }

  public int getCountriesNextToOceanCount(int oid) {
    try {
      ps = connection.prepareStatement("SELECT COUNT(*) FROM oceanAccess WHERE oid=?;");
      ps.setInt(1, oid);
      rs = ps.executeQuery();
      rs.next();
      int numCountries = rs.getInt(1);
      ps.close();
      rs.close();
      ps = null;
      rs = null;
      return numCountries;
    } catch (SQLException e) {
      e.printStackTrace();
      return -1;
    }
  }

  public String getOceanInfo(int oid){
    String oceanInfo = "";
    try {
      ps = connection.prepareStatement("SELECT * FROM ocean WHERE oid=?;");
      ps.setInt(1, oid);
      rs = ps.executeQuery();
      rs.next();
      oceanInfo = rs.getInt(1) + ":" + rs.getString(2) + ":" + rs.getInt(3);
      ps.close();
      rs.close();
      ps = null;
      rs = null;
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return oceanInfo;
  }

  public boolean chgHDI(int cid, int year, float newHDI){
    try {
      ps = connection.prepareStatement("UPDATE hdi SET hdi_score = ? WHERE cid=? AND year=?;");
      ps.setFloat(1, newHDI);
      ps.setInt(2, cid);
      ps.setInt(3, year);
      ps.executeUpdate();
      ps.close();
      ps = null;
      return true;
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
    try {
      ps = connection.prepareStatement("DELETE FROM neighbour WHERE "
        + "country IN (?, ?) AND neighbor IN (?, ?);");
      ps.setInt(1, c1id);
      ps.setInt(2, c2id);
      ps.setInt(3, c1id);
      ps.setInt(4, c2id);
      ps.execute();
      ps.close();
      ps = null;
      return true;
    } catch (SQLException e) {
      e.printStackTrace();
      return false;        
    }
  }

  public String listCountryLanguages(int cid){
    String languages = "";
    try {
      ps = connection.prepareStatement("SELECT l.lid, l.lname, c.population*l.lpercentage "
        + "AS population FROM country AS c JOIN language "
        + "AS l ON c.cid=l.cid WHERE c.cid=? ORDER BY population DESC;");
      ps.setInt(1, cid);
      rs = ps.executeQuery();
      while (rs.next()){
        languages += rs.getInt(1) + ":" + rs.getString(2) + ":" + rs.getFloat(3);
        if (!rs.isLast())
          languages += "#";
      }
      rs.close();
      ps.close();
      rs = null;
      ps = null;
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return languages;
  }

  public boolean updateHeight(int cid, int decrH){
    try {
      ps = connection.prepareStatement("UPDATE country SET height = ? WHERE cid = ?");
      ps.setInt(1, decrH);
      ps.setInt(2, cid);
      ps.executeUpdate();
      ps.close();
      ps = null;
      return true;
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
  }

  public boolean updateDB(){
    try {
      ps = connection.prepareStatement("CREATE TABLE IF NOT EXISTS "
        + "mostPopulousCountries(cid INTEGER, cname VARCHAR(20));");
      ps.executeUpdate();
      ps.close();
      ps = connection.prepareStatement("INSERT INTO "
        + "mostPopulousCountries (SELECT cid, cname FROM country WHERE population "
          + ">= 100000000 ORDER BY cid ASC);");
      ps.execute();
      ps.close();
      ps = null;
      return true;
    } catch (SQLException e) {
      e.printStackTrace();
      return false; 
    }   
  }

}
