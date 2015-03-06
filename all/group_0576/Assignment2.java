import java.sql.*;

public class Assignment2 {

  // A connection to the database  
  Connection connection = null;
  
  //CONSTRUCTOR
  Assignment2() throws ClassNotFoundException{
    try {
      Class.forName("org.postgresql.Driver");
    }catch (ClassNotFoundException e) {
		
    }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password) throws SQLException{
    try {
        connection = DriverManager.getConnection(URL, username, password);
        if (connection != null)
          return true;
     }catch (SQLException e) {
        return false;
     }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB() throws SQLException{  
    try {
      if (connection != null){
        connection.close();
        return true;
        }
      return false; 
    }catch (SQLException e){
      return false;    
    }
  }

  public boolean insertCountry (int cid, String name, int height, int population) throws SQLException{
    Statement sql = null;
    try {
      String query = "INSERT INTO a2.country (cid, cname, height, population) VALUES" + 
      "(" + cid + ", '" + name + "', " + height + ", " + population + ")";
      sql = connection.createStatement();
      sql.executeUpdate(query);
      return true;
    }catch (SQLException e){
      return false;   
    }finally {
      if (sql != null){
        sql.close();
      }
    }
  }
  
  public int getCountriesNextToOceanCount(int oid) throws SQLException{
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      int retNumber = 0;
      String query = "SELECT COUNT(cid) AS number FROM a2.oceanAccess WHERE oid = " + oid ;
      ps = connection.prepareStatement(query);
      rs = ps.executeQuery();

      while (rs.next()){
        retNumber = rs.getInt("number");
      }
      return retNumber;
    }catch (SQLException e){
      return -1;
    }finally {
      if (ps != null)
        ps.close();
    }
  }

  public String getOceanInfo(int oid) throws SQLException{
    PreparedStatement ps = null;
    ResultSet rs = null;
    try{
      String query = "SELECT * FROM a2.ocean WHERE oid = " + oid;
      String retString = "";
      ps = connection.prepareStatement(query);
      rs = ps.executeQuery();

      while(rs.next()) {
        retString += oid + ":" + rs.getString("oname") + ":" + rs.getInt("depth");
      }
      return retString;
    }catch(SQLException e){
     return "";
    }finally {
    if (ps != null)
      ps.close();
    }
  }

  public boolean chgHDI(int cid, int year, float newHDI) throws SQLException{
    Statement sql = null;
    try {
      String query = "UPDATE a2.hdi SET hdi_score = " + newHDI + " WHERE cid = " + cid + "AND year = " + year;
      sql = connection.createStatement();
      sql.executeUpdate(query);
      return true;
    }catch(SQLException e){
     return false;
   }finally {
    if (sql != null)
      sql.close();
    }
  }

  public boolean deleteNeighbour(int c1id, int c2id) throws SQLException{
    Statement sql = null;
    try {
      String query = "DELETE FROM a2.neighbour WHERE (country = " + c1id + "AND neighbor = " + c2id +
      ") OR (country = " + c2id + "AND neighbor = " + c1id + ")";
      sql = connection.createStatement();
      sql.executeUpdate(query);
      return true;
    }catch (SQLException e){
      return false;           
    }finally{
      if(sql != null)
        sql.close();
    }
  }

  public String listCountryLanguages(int cid) throws SQLException{
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      String lname = "";
      float lpercentage = 0;
      int population = 0, lid = 0, lpopulation = 0;
      String ret_String = "";


      String query = "SELECT population FROM a2.country WHERE cid = " + cid;
      ps = connection.prepareStatement(query);
      rs = ps.executeQuery();
      while (rs.next())
        population = rs.getInt("population");

      query = "SELECT * FROM a2.language WHERE cid = " + cid;
      ps = connection.prepareStatement(query);
      rs = ps.executeQuery();
      while (rs.next()){
        lid = rs.getInt("lid");
        lname = rs.getString("lname");
        lpercentage = rs.getFloat("lpercentage");
        lpopulation = population * lpercentage;
        ret_String = lid + ":" + lname + ":" + lpopulation + "#";
      }
      return ret_String;
    }catch (SQLException e) {
      return "";    
    }finally{
      if (ps != null)
        ps.close();
    }
  }

  //return true and false is reverse
  public boolean updateHeight(int cid, int decrH) throws SQLException{
    Statement sql = null;
    try {
      String query = "UPDATE a2.country SET height = height - " + decrH + "WHERE cid = " + cid;
      sql = connection.createStatement();
      sql.executeQuery(query);
      return true;
    }catch (SQLException e) {
      return false;
    }finally {
      if (sql != null)
        sql.close();
    }
  }

  public boolean updateDB() throws SQLException{
    Statement sql = null;
    try {
      String query = "CREATE TABLE a2.mostPopulousCountries " +
      "(cid INTEGER, cname VARCHAR(20))";
      sql = connection.createStatement();
      sql.execute(query);

      query = "INSERT INTO a2.mostPopulousCountries (cid, cname)" + 
      "SELECT cid, cname FROM country WHERE population > 1000000 ORDER BY cid";
      sql.executeQuery(query);
      return true;
    }catch (SQLException e) {
      return false;
    }finally {
      if (sql != null)
        sql.close();
    }
  }
}