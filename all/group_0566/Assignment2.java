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
  Assignment2() {
    try {
      Class.forName("org.postgresql.jdbc.Driver");
    } catch (ClassNotFoundException e) {
    }
  }
  
  //Using the input parameters, establish a connection to be used for this 
  //session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
    try {
      connection = DriverManager.getConnection(URL, username, password);
      return true;
    } catch (SQLException e) {
      return false;
    }  
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){

      try {
        connection.close();
        return true;
      } catch (SQLException e) {
        return false;
      }   
  }
    
  public boolean insertCountry (int cid, String name, int height, 
      int population) {
    
    try {
      ps = connection.prepareStatement("INSERT INTO " +
      		"a2.country VALUES(?, ?, ?, ?);");
      ps.setInt(1, cid);
      ps.setString(2, name);
      ps.setInt(3, height);
      ps.setInt(4, population);
      if(ps.executeUpdate() == 0) {
        return false;
      };
      return true;
    } catch (SQLException e) {
      return false;

    }
    
  }
  
  public int getCountriesNextToOceanCount(int oid) {
    try {
      ps = connection.prepareStatement(
          "SELECT count(DISTINCT cid) as numCountries " +
          "FROM a2.oceanAccess " +
          "WHERE oid = ?;");
      ps.setInt(1, oid);
      rs = ps.executeQuery();
      if (rs.next()) {return rs.getInt("numCountries");}
      
    } catch (SQLException e) {
      return -1;
    } 
    return -1; 
  }
   
  public String getOceanInfo(int oid){
    try {
      ps = connection.prepareStatement(
          "SELECT oid, oname, depth " +
          "FROM a2.ocean " +
          "WHERE oid = ?;");
      ps.setInt(1, oid);
      rs = ps.executeQuery();
      if (rs.next()) {
      String returnStr = rs.getInt("oid") + ":" + rs.getString("oname") + ":" + 
      rs.getInt("depth");
      
      
      if (returnStr.isEmpty()) {
        return "";
      }
      
      return returnStr;
      }
    } catch (SQLException e) {
    }
   return "";
  }

  public boolean chgHDI(int cid, int year, float newHDI){
    try {
      ps = connection.prepareStatement(
          "UPDATE a2.hdi " +
          "SET hdi_score = ? " +
          "WHERE cid = ? and year = ?;");
      ps.setFloat(1, newHDI);
      ps.setInt(2, cid);
      ps.setInt(3, year);
      if(ps.executeUpdate() == 0){
        return false;
      }
        
      return true;
    } catch (SQLException e) {
      return false;
    }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
   try {
    ps = connection.prepareStatement(
         "DELETE FROM a2.neighbour " +
         "WHERE (country = ? AND neighbor = ?) OR (country = ? AND neighbor " +
         "= ?);");
    ps.setInt(1, c1id);
    ps.setInt(2, c2id);
    ps.setInt(3, c2id);
    ps.setInt(4, c1id);
    if(ps.executeUpdate() == 0) {
      return false;
    }
    
   return true;
   
  } catch (SQLException e) {
    return false;
  }
           
  }
  
  public String listCountryLanguages(int cid){
    try {
      ps = connection.prepareStatement(
          "SELECT lid, lname, (lpercentage * population) AS population " +
          "FROM a2.language, a2.country " +
          "WHERE language.cid = country.cid AND language.cid = ? " +
          "ORDER BY population;");
      ps.setInt(1, cid);
      rs = ps.executeQuery();
      String returnStr = "";
      while(rs.next()) {
          int lid = rs.getInt("lid");
          String lname = rs.getString("lname");
          int lpop = rs.getInt("population");
          returnStr = returnStr + lid + ":" + lname + ":" + lpop + "#";
          
      }
      if (returnStr.length() != 0) {
          returnStr = returnStr.substring(0, returnStr.length() -1) ;    
        }
      return returnStr;
      
    } catch (SQLException e) {
      return "";
    }
    
  }
  
  public boolean updateHeight(int cid, int decrH){
    try {
          ps = connection.prepareStatement(
              "UPDATE a2.country " +
              "SET height = height - ? " +
              "WHERE cid = ?;");
          ps.setFloat(1, decrH);
          ps.setInt(2, cid);
          if(ps.executeUpdate() == 0) {
            return false;
          }
      return true;
  } catch (SQLException e) {
        return false;
      }
  }
  
    
  public boolean updateDB(){
    try {
      ps = connection.prepareStatement("CREATE TABLE " +
      		"a2.mostPopulousCountries(cid INTEGER, cname VARCHAR(20))");
      ps.executeUpdate();
      ps = connection.prepareStatement("INSERT INTO a2.mostPopulousCountries(" +
      		"SELECT cid, cname " +
            "FROM a2.country " +
            "WHERE population > 100000000 " +
            "ORDER BY cid);");
      
      if(ps.executeUpdate() == 0) {
        return false;
      }
      return true;
    } catch (SQLException e){   
      
      return false;
    }    
  }
}