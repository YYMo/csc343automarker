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

  //Query string to execute
  String query;
  
  //CONSTRUCTOR
  Assignment2() {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      System.out.println("Failed to find the JDBC driver.");
    }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
    try {
      connection = DriverManager.getConnection(URL, username, password);
      return true;
    } catch (SQLException se) {
      return false;
    }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
    try {
      connection.close();
      return true;
    } catch (SQLException se) {
      return false;    
    }
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
    try {
      query = "INSERT INTO a2.country VALUES(?,?,?,?);";
      ps = connection.prepareStatement(query);
      ps.setInt(1, cid);
      ps.setString(2, name);
      ps.setInt(3, height);
      ps.setInt(4, population);
      ps.executeUpdate();
      closePreparedStatement(ps);
      return true;
    } catch (SQLException se) {
      return false;
    }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
    try{
      query = "SELECT count(cid) FROM a2.oceanAccess WHERE oid=?;";
      ps = connection.prepareStatement(query);
      ps.setInt(1, oid);
      rs = ps.executeQuery();
      rs.next();
      int count = rs.getInt("count");
      closePreparedStatement(ps);
      closeResultSet(rs);
      return count;
    } catch (SQLException se) {
      return -1;
    }
  }
   
  public String getOceanInfo(int oid){
    try {
      query = "SELECT * FROM a2.ocean WHERE oid=?";
      ps = connection.prepareStatement(query);
      ps.setInt(1, oid);
      rs = ps.executeQuery();
      String result = "";
      //get a single line
      while (rs.next()) {
        result += rs.getInt("oid") + ":" + rs.getString("oname") + ":" + rs.getInt("depth");
      }
      closePreparedStatement(ps);
      closeResultSet(rs);
      return result;
    } catch (SQLException se) {
      return "";
    }
  }

  public boolean chgHDI(int cid, int year, float newHDI){
    try {
      query = "UPDATE a2.hdi SET hdi_score=? WHERE cid=? AND year=?;";
      ps = connection.prepareStatement(query);
      ps.setFloat(1, newHDI);
      ps.setInt(2, cid);
      ps.setInt(3, year);
      int status = ps.executeUpdate();
      closePreparedStatement(ps);
      if (status == 1)
        return true;
      else
        return false;
    } catch (SQLException se) {
      return false;
    }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
    try {
      //delete the (c1id, c2id) combination from neighbour
      query = "DELETE FROM a2.neighbour WHERE country=? AND neighbor=?;";
      ps = connection.prepareStatement(query);
      ps.setInt(1, c1id);
      ps.setInt(2, c2id);
      int firstDelete = ps.executeUpdate();
      //delete the (c2id, c2id) combination from neighbour
      query = "DELETE FROM a2.neighbour WHERE country=? AND neighbor=?;";
      ps = connection.prepareStatement(query);
      ps.setInt(1, c2id);
      ps.setInt(2, c1id);
      int secondDelete = ps.executeUpdate();
      closePreparedStatement(ps);
      if (firstDelete == 1 && secondDelete == 1)
        return true;
      else
        return false;
    } catch (SQLException se) {
      return false;
    }
  }
  
  public String listCountryLanguages(int cid){
    try { //DrOP VIEW IF EXISTS
      query = "DROP VIEW IF EXISTS a2.languagePopulation CASCADE; " + 
        "CREATE VIEW a2.languagePopulation AS " +
        "SELECT t1.cid, t1.lid, t1.lname, " +
        "t1.lpercentage * t2.population AS population " +
        "FROM a2.language t1 JOIN a2.country t2 ON " +
        "t1.cid = t2.cid;";
      ps = connection.prepareStatement(query);
      ps.executeUpdate();
      query = "SELECT lid, lname, population " +
      "FROM a2.languagePopulation " +
      "WHERE cid=? ORDER BY population;";
      ps = connection.prepareStatement(query);
      ps.setInt(1, cid);
      rs = ps.executeQuery();
      String result = "";
      while (rs.next()) {
        result += rs.getInt("lid") + ":" + rs.getString("lname") + ":" + rs.getInt("population") + "#";
      }
      try {
        result = result.substring(0, result.length() - 1); //remove the last # if there was data
      } catch (StringIndexOutOfBoundsException e) {
        result = ""; //The exception happens when there is no info to retrieve
      }
      ps = connection.prepareStatement("DROP VIEW a2.languagePopulation;");
      ps.executeUpdate();
      closePreparedStatement(ps);
      closeResultSet(rs);
      return result;
    } catch (SQLException se) {
      return "";
    }
  }
  
  public boolean updateHeight(int cid, int decrH){
    try {
      int height = 0;
      //get height of country
      query = "SELECT height FROM a2.country WHERE cid=?;";
      ps = connection.prepareStatement(query);
      ps.setInt(1, cid);
      rs = ps.executeQuery();
      rs.next();
      height = rs.getInt("height");
      int newHeight = height - decrH;
      query = "UPDATE a2.country SET height=? WHERE cid=?;";
      ps = connection.prepareStatement(query);
      ps.setInt(1, newHeight);
      ps.setInt(2, cid);
      int status = ps.executeUpdate();
      closePreparedStatement(ps);
      closeResultSet(rs);
      if (status == 1)
        return true;
      else
        return false;
    } catch (SQLException se) {
     return false;
    }
  }
    
  public boolean updateDB(){
    try {
      query = "DROP TABLE IF EXISTS a2.mostPopulousCountries CASCADE; " + 
        "CREATE TABLE a2.mostPopulousCountries (cid INTEGER NOT NULL, " +
        "cname VARCHAR(20) NOT NULL); " + 
        "INSERT INTO a2.mostPopulousCountries " + 
        "SELECT cid, cname FROM a2.country " + 
        "WHERE population > 100000;";
      ps = connection.prepareStatement(query);
      ps.executeUpdate();
      closePreparedStatement(ps);
      return true;
    } catch (SQLException se) {
      return false;
    }
  }

  public void closeResultSet (ResultSet rs) {
    try {
    if (!rs.isClosed()) 
      rs.close();
    } catch (SQLException se) {

    }
  }

  public void closePreparedStatement (PreparedStatement ps) {
    try {
      if (!ps.isClosed()) 
        ps.close();
    } catch (SQLException se) {

    }
  }
}
