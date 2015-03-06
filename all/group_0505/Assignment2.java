

import java.io.IOException;
import java.sql.*;

public class Assignment2 {
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
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  
  //Using the input parameters, establish a connection to be used for this session. 
  //Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
    try{
      connection = DriverManager.getConnection(URL, username, password);
    } catch (SQLException e) {
      return false;
    }
    return true;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
    try {
      connection.close();
    } catch (SQLException e){
      return false;    
    }
    return true;
  }
    
  public boolean insertCountry (int cid, String name, int height, 
      int population) {
    try {   
      Statement st = connection.createStatement();           
      String SQL = "SELECT cid from a2.country WHERE cid ="+ cid;           
      ResultSet rs1 = st.executeQuery(SQL);
      boolean contain = rs1.next();
      st.close();
      rs1.close();
      if (contain){
        return false;
      } else {
        ps = connection.prepareStatement(
            "INSERT INTO a2.country(cid, cname, height, population) "
                + "VALUES(?,?,?,?)");
        ps.setInt(1, cid);
        ps.setString(2, name);
        ps.setInt(3, height);
        ps.setInt(4, population);
        if (ps.executeUpdate() == 0) {
          return false;
        }
        ps.close();
      }
    } catch(SQLException e){
      return false;
    } 
    return true;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
    try {
      ps = connection.prepareStatement("SELECT COUNT(cid) AS num FROM "
          + "a2.oceanAccess WHERE oid=" + oid + "" + "GROUP BY oid");
      rs = ps.executeQuery();
      
      if (rs.next()) {
        return rs.getInt("num");
      } else {
        return -1;
      }
    } catch (SQLException e) {
      return -1; 
    } finally {
      try {
        if (ps != null)
          ps.close();
      } catch (SQLException e) {
        return -1;
      }
      try {
        if (rs != null) 
          rs.close();
      } catch (SQLException e) {
        return -1;
      }
    }
  }
   
  public String getOceanInfo(int oid){
    try {
      ps = connection.prepareStatement("SELECT oid, oname, depth FROM "
          + "a2.ocean WHERE oid=" + oid);
      rs = ps.executeQuery();
      if (rs.next()) {
        return rs.getString("oid") + ":" + rs.getString("oname") + ":"
            + rs.getString("depth");  
      } else {
        return "";
      }
    } catch (SQLException e) {
      return "";
    } finally {
      try {
        if (ps != null)
          ps.close();
      } catch (SQLException e) {
        return "";
      }
      try {
        if (rs != null)
          rs.close();
      } catch (SQLException e) {
        return "";
      }
    }
  }

  public boolean chgHDI(int cid, int year, float newHDI){
   try {
     ps = connection.prepareStatement("UPDATE a2.hdi SET hdi_score=" + newHDI 
         + "WHERE cid=" + cid + "AND year=" + year);
     if (ps.executeUpdate() == 0) {
       return false;
     }
     ps.close();
   } catch (SQLException e) {
     return false;
   }
   return true;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
   try {
     ps = connection.prepareStatement("DELETE FROM a2.neighbour where (country="
         + c1id + "AND neighbor=" + c2id + ")"
         + "OR (country=" + c2id + "AND neighbor=" + c1id +")");
     if (ps.executeUpdate() == 0) {
       return false;}
     ps.close();
   } catch (SQLException e) {
     return false;
   }
   return true;
  }
  
  public String listCountryLanguages(int cid){
    String ans = "";
    try {
      ps = connection.prepareStatement("SELECT c1.lid AS lid, c1.lname AS lname,"
          + "c1.lpercentage * c2.population AS population FROM "
          + "a2.language c1, a2.country c2 WHERE c1.cid = c2.cid "
          + " AND c1.cid=" + cid + "ORDER BY "
          + "population DESC");
      rs = ps.executeQuery();
      while (rs.next()) {
        ans += rs.getString("lid") + ":" + rs.getString("lname") + ":" 
            + rs.getInt("population") + "#";
      }
      ps.close();
      rs.close();
    } catch (SQLException e) {
      return "";
    } 
    return ans;
  }
  
  public boolean updateHeight(int cid, int decrH){
    try {
      ps = connection.prepareStatement("UPDATE a2.country SET height=" + decrH 
          + "WHERE cid=" + cid);
      if (ps.executeUpdate() == 0) {
        return false;
      }
      ps.close();
    } catch (SQLException e) {
      return false;
    }
    return true;
  }
    
  public boolean updateDB(){
    try {
      ps = connection.prepareStatement("CREATE TABLE a2.mostPopulousCountries AS "
          + "SELECT cid, cname FROM a2.country WHERE population > 1000000 "
          + "ORDER BY cid");
      ps.execute();
      ps.close();
    } catch (SQLException e) {
      return false;
    }
    return true;    
  }
  
}
