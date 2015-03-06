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
      connection = DriverManager.getConnection(URL, username, password);
      return true;
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
    try {
      connection.close();
      return true;
    } catch (SQLException e) {
      e.printStackTrace();
      return false;    
    }
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
    try {
      sql = connection.createStatement();
      String sqlText;
      sqlText = String.format("SELECT * FROM country WHERE cid = %d", cid);
      rs = sql.executeQuery(sqlText);
      if (!rs.next()) {
        sqlText = String.format("INSERT INTO country VALUES (%d, \'%s\', %d, %d)", cid, name, height, population);
        sql.executeUpdate(sqlText);
        return true;
      }
      rs.close();
      sql.close();
      return false;
    } catch (SQLException e) {
      e.printStackTrace();
      rs.close();
      sql.close();
      return false;
    }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
    try {
      sql = connection.createStatement();
      String sqlText;
      sqlText = String.format("SELECT count(*) number FROM oceanAccess WHERE oid = %d", oid);
      rs = sql.executeQuery(sqlText);
      if (rs.next()) {
        int number = rs.getInt(1);
        rs.close();
        sql.close();
        return number;
      }
      rs.close();
      sql.close();
      return -1;
    } catch (SQLException e) {
      e.printStackTrace();
      rs.close();
      sql.close();
      return -1;
    } 
  }
   
  public String getOceanInfo(int oid){
    try {
      sql = connection.createStatement();
      String sqlText;
      sqlText = String.format("SELECT * FROM ocean WHERE oid = %d", oid);
      rs = sql.executeQuery(sqlText);
      if (rs.next()) {
        String oceanInfo;
        oceanInfo = String.format("%d:%s:%d", rs.getInt(1), rs.getString(2), rs.getInt(3));
        rs.close();
        sql.close();
        return oceanInfo;
      }
      rs.close();
      sql.close();
      return "";
    } catch (SQLException e) {
      e.printStackTrace();
      rs.close();
      sql.close();
      return "";
    }
  }

  public boolean chgHDI(int cid, int year, float newHDI){
    try {
      sql = connection.createStatement();
      String sqlText;
      sqlText = String.format("UPDATE hdi SET hdi_score = %f WHERE cid = %d AND year = %d", newHDI, cid, year);
      int rowsUpdated = sql.executeUpdate(sqlText);
      if (rowsUpdated == 1) {
        sql.close();
        return true;
      }
      sql.close();
      return false;
    } catch (SQLException e) {
      e.printStackTrace();
      sql.close();
      return false;
    }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
    try {
      sql = connection.createStatement();
      String sqlText;
      sqlText = String.format("DELETE FROM neighbour WHERE (country = %d AND neighbor = %d) OR (country = %d AND neighbor = %d)", c1id, c2id, c2id, c1id);
      int rowsDeleted = sql.executeUpdate(sqlText);
      if (rowsDeleted == 2) {
        sql.close();
        return true;
      }
      sql.close();
      return false;
    } catch (SQLException e) {
      e.printStackTrace();
      sql.close();
      return false;
    }     
  }
  
  public String listCountryLanguages(int cid){
    try {
      sql = connection.createStatement();
      String sqlText;
      sqlText = String.format("SELECT lid, lname, cast(round(population*lpercentage, 0) AS Integer) population FROM country INNER JOIN language ON country.cid = language.cid WHERE country.cid = %d ORDER BY population", cid);
      rs = sql.executeQuery(sqlText);
      String countryLang = "";
      String temp;
      if (rs.next()) {
        temp = String.format("%d:%s:%d", rs.getInt(1), rs.getString(2), rs.getInt(3));
        countryLang = countryLang.concat(temp);
      }
      while (rs.next()) {
        temp = String.format("#%d:%s:%d", rs.getInt(1), rs.getString(2), rs.getInt(3));
        countryLang = countryLang.concat(temp); 
      }
      rs.close();
      sql.close();
      return countryLang;
    } catch (SQLException e) {
      e.printStackTrace();
      rs.close();
      sql.close();
      return "";
    }
  }
  
  public boolean updateHeight(int cid, int decrH){
    try {
      sql = connection.createStatement();
      String sqlText;
      sqlText = String.format("UPDATE country SET height = height - %d WHERE cid = %d", decrH, cid);
      int rowsUpdated = sql.executeUpdate(sqlText);
      if (rowsUpdated == 1) {
        sql.close();
        return true;
      }
      sql.close();
      return false;
    } catch (SQLException e) {
      e.printStackTrace();
      sql.close();
      return false;
    }
  }
    
  public boolean updateDB(){
    try {
      sql = connection.createStatement();
      String sqlText;
      sqlText = "CREATE TABLE mostPopulousCountries( cid int, cname varchar(20) )";
      sql.executeUpdate(sqlText);
      sqlText = "INSERT INTO mostPopulousCountries (SELECT cid, cname FROM country WHERE population > 100000000 ORDER BY cid)";
      sql.executeUpdate(sqlText);
      sql.close();
      return true;
    } catch (SQLException e) {
      e.printStackTrace();
      sql.close();
      return false;
    }  
  }
  
}



