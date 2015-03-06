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
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try{
          Class.forName("org.postgresql.Driver");
      }
      catch (ClassNotFoundException e) {
          System.out.println("Failed to find the driver");
          return false;
      }
      try{
          connection = DriverManager.getConnection(URL, username, password);
          return true;
      }
      catch (SQLException e) {
          System.out.println("Failed to connect");
          return false;
      }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try{
          System.out.println("Disconecting...");
          connection.close();
          return true;
      }
      catch (SQLException e) {
          System.out.println("Failed to disconnect");
          return false;
      }
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
      try{
          String s = "INSERT INTO country" + "VALUES(" + cid + "," + name +
          "," + height + "," + population + ")";
          System.out.println("Executing SQL: " + s);
          sql = connection.createStatement(); 
          sql.executeUpdate(s);
          if (!sql.isClosed()) {
            sql.close();
          }
          if (!rs.isClosed()) {
            rs.close();
          }
          if (!ps.isClosed()) {
            ps.close();
          }
          return true;
      }
      catch (SQLException e) {
          System.out.println("Failed to insertCountry");
          return false;
      }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
    try{
      int result = 0;
      String s = "SELECT count(o.oid)" + " FROM oceanAccess o WHERE o.oid = " + oid;
      sql = connection.createStatement();
      System.out.println("Executing SQL: " + s);
      rs = sql.executeQuery(s);
      if (rs.first()) {
        result = rs.getInt("count");
      }
      else {
        result = -1;
      }
      if (!sql.isClosed()) {
        sql.close();
      }
      if (!rs.isClosed()) {
        rs.close();
      }
      if (!ps.isClosed()) {
        ps.close();
      }
      return result;
    }
    catch (SQLException e) {
      System.out.println("Failed to get countries next to ocean");
      return -1;
    }  
  }
  
  public String getOceanInfo(int oid){
    try{
      String result = "";
      String s = "SELECT * FROM ocean o WHERE o.oid = " + oid;
      sql = connection.createStatement();
      System.out.println("Executing SQL: " + s);
      rs = sql.executeQuery(s);
      if (rs.first()) {
        result = oid + ":" + rs.getString("oname") + ":" + rs.getInt("depth");
      }
      else {
        result = "";
      }
      if (!sql.isClosed()) {
        sql.close();
      }
      if (!rs.isClosed()) {
        rs.close();
      }
      if (!ps.isClosed()) {
        ps.close();
      }
      return result;
    }
    catch (SQLException e) {
      System.out.println("Failed to get ocean information");
      return "";
    }  
  }

  public boolean chgHDI(int cid, int year, float newHDI){
    try{
      String s = "UPDATE hdi SET year = " + year + "," + " hdi_score = " + newHDI + " WHERE cid = " + cid;
      sql = connection.createStatement();
      System.out.println("Executing SQL: " + s);
      sql.executeUpdate(s);
      if (!sql.isClosed()) {
        sql.close();
      }
      if (!rs.isClosed()) {
        rs.close();
      }
      if (!ps.isClosed()) {
        ps.close();
      }
      return true;
    }
    catch (SQLException e) {
      System.out.println("Failed to change HDI");
      return false;
    }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
    try{
      String s1 = "DELETE FROM neighbour WHERE country = " + c1id + " AND neighbor = " + c2id;
      String s2 = "DELETE FROM neighbour WHERE country = " + c2id + " AND neighbor = " + c1id;
      sql = connection.createStatement();
      System.out.println("Executing SQL: " + s1);
      sql.executeUpdate(s1);
      System.out.println("Executing SQL: " + s2);
      sql.executeUpdate(s2);
      if (!sql.isClosed()) {
        sql.close();
      }
      if (!rs.isClosed()) {
        rs.close();
      }
      if (!ps.isClosed()) {
        ps.close();
      }
      return true;
    }
    catch (SQLException e) {
      System.out.println("Failed to delete");
      return false;
    }       
  }
  
  public String listCountryLanguages(int cid){
    try{
      String result = "";
      String s = "SELECT l.lid, l.lname, (lpercentage * c.population) AS population FROM country c, language l " + 
      "WHERE c.cid = l.cid AND c.cid = " + cid;
      sql = connection.createStatement();
      System.out.println("Executing SQL: " + s);
      rs = sql.executeQuery(s);
      while (rs.next()) {
        result += rs.getInt("lid") + ":" + rs.getString("lname") + ":" + rs.getInt("population") + "#";
      }
      if (!sql.isClosed()) {
        sql.close();
      }
      if (!rs.isClosed()) {
        rs.close();
      }
      if (!ps.isClosed()) {
        ps.close();
      }
      if (result == "") {
        return result;
      }
      else {
        return result.substring(0, result.length() - 1);
      }
    }
    catch (SQLException e) {
      System.out.println("Failed to list country languages");
      return "";
    }
  }
  
  public boolean updateHeight(int cid, int decrH){
    try {
      String s = "UPDATE country SET height = " + decrH + " WHERE cid = " + cid;
      sql = connection.createStatement();
      System.out.println("Executing SQL: " + s);
      sql.executeUpdate(s);
      if (!sql.isClosed()) {
        sql.close();
      }
      if (!rs.isClosed()) {
        rs.close();
      }
      if (!ps.isClosed()) {
        ps.close();
      }
      return true;
    }
    catch (SQLException e) {
      System.out.println("Failed to update height");
      return false;
    }
  }
    
  public boolean updateDB(){
    try {
      String s1 = "SELECT c.cid, c.cname FROM country c WHERE c.population > 100000000 ORDER BY c.cid ASC";
      sql = connection.createStatement();
      System.out.println("Executing SQL: " + s1);
      rs = sql.executeQuery(s1);
      String s2 = "CREATE TABLE mostPopulousCountries (cid INTEGER, cname VARCHAR(20))";
      System.out.println("Executing SQL: " + s2);
      sql.executeUpdate(s2);
      String s3 = "INSERT INTO mostPopulousCountries VALUES (?, ?)";
      ps = connection.prepareStatement(s3);
      while(rs.next()) {
        ps.setInt(1, rs.getInt("cid"));
        ps.setString(2, rs.getString("cname"));
        ps.executeUpdate();
      }
      if (!sql.isClosed()) {
        sql.close();
      }
      if (!rs.isClosed()) {
        rs.close();
      }
      if (!ps.isClosed()) {
        ps.close();
      }
      return true;
    }
    catch (SQLException e) {
      System.out.println("Failed to updateDB");
      return false; 
    }
   
  }
  
}
