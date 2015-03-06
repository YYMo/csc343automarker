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
  
  String sqlText;

  //CONSTRUCTOR
  public Assignment2() throws ClassNotFoundException {
    try{
      Class.forName("org.postgresql.Driver");
    }
    catch (Throwable e) {
      // c.printStackTrace();
    }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password) {
    try {
      connection = DriverManager.getConnection(URL, username, password);
      return true;
    } catch (SQLException s) {
      return false;
    }
    
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB() {
    try {
      if (connection != null) {
        connection.close();        // ?????????????
        return true;
      }
    } catch (SQLException s) {
      //
    }
    return false;
        
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
    try {
      sqlText = "INSERT INTO a2.country VALUES (?, ?, ?, ?)";
      ps = connection.prepareStatement(sqlText);
      ps.setInt(1, cid);
      ps.setString(2, name);
      ps.setInt(3, height);
      ps.setInt(4, population);
      rs = ps.executeQuery();
      ps.close();
      rs.close();
      return true;
    } catch (SQLException s) {
      //  
    } 
    return false;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
    try {
      sqlText = "SELECT * FROM a2.oceanaccess WHERE oid=?";
      ps = connection.prepareStatement(sqlText);
      ps.setInt(1, oid);
      rs = ps.executeQuery();
      int count = 0;
      while (rs.next()) {
        count += 1;
      }
      ps.close();
      rs.close();
      return count;
    } catch (SQLException s) {
      // s.printStackTrace();
    }
    return -1;
  }
   
  public String getOceanInfo(int oid) {
    String info = "";
    try {
      sqlText = "SELECT * FROM a2.ocean where oid=?";
      ps = connection.prepareStatement(sqlText);
      ps.setInt(1, oid);
      rs = ps.executeQuery();
      rs.next();
      info += rs.getInt("oid") + ":";
      info += rs.getString("oname") + ":";
      info += rs.getInt("depth");
      ps.close();
      rs.close();
    } catch (SQLException s) {
      // s.printStackTrace();
    }
    return info;
  }

  public boolean chgHDI(int cid, int year, float newHDI) {
    try {
      sqlText = "UPDATE a2.hdi SET hdi_score=? WHERE (cid=? AND year=?)";      
      ps = connection.prepareStatement(sqlText);
      ps.setFloat(1, newHDI);
      ps.setInt(2, cid);
      ps.setInt(3, year);
      // rs = ps.executeQuery();
      if (ps.executeUpdate() == 0) { // no rows were updated
        return false;
      }
      ps.close();
      return true;
    } catch (SQLException s) {
      // s.printStackTrace();
    }
  return false;
  }

  public boolean deleteNeighbour(int c1id, int c2id) {
    try {
      sqlText = "DELETE FROM a2.neighbour WHERE country=?";
      ps = connection.prepareStatement(sqlText);
      ps.setInt(1, c1id);
      ps.executeUpdate();
      ps.close();
      sqlText = "DELETE FROM a2.neighbour WHERE country=?";
      ps = connection.prepareStatement(sqlText);
      ps.setInt(1, c2id);
      ps.executeUpdate();
      ps.close();
      return true;
    } catch (SQLException s) {
      // s.printStackTrace();
    } 
    return false;
  }
  
  public String listCountryLanguages(int cid) {
    String info = "";
    try {
      sql = connection.createStatement();
      sqlText = "CREATE VIEW a2.tempview AS SELECT cid, lid, lname, population*lpercentage AS population FROM (SELECT cid, population FROM a2.country) AS temp1 NATURAL JOIN (SELECT * FROM a2.language) AS temp2 ORDER BY population;";
      sql.executeUpdate(sqlText);
      sqlText = "SELECT * FROM a2.tempview WHERE cid=?";
      ps = connection.prepareStatement(sqlText);
      ps.setInt(1, cid);
      // if (ps.executeQuery() == 0) { // no country with the given cid
      //   return info;
      // }
      rs = ps.executeQuery();
      int count = 1;
      while (rs.next()) {
        info += rs.getInt("lid") + ":";
        info += rs.getString("lname") + ":";
        info += rs.getString("population") + "#";
      }
      rs.close();
      ps.close();
      sqlText = "DROP VIEW a2.tempview;";
      sql.executeUpdate(sqlText);
      sql.close();    
    } catch (SQLException s) {
      // s.printStackTrace();
    }
    try {
      sql = connection.createStatement();
      sqlText = "DROP VIEW IF EXISTS  a2.tempview CASCADE;";
      sql.executeUpdate(sqlText);
      sql.close();
    } catch (SQLException s) {
      // s.printStackTrace();
    }    
    return info;
  }
  
  public boolean updateHeight(int cid, int decrH) {
    try {
      sqlText = "SELECT cid, height FROM a2.country WHERE cid=?";
      ps = connection.prepareStatement(sqlText);
      ps.setInt(1, cid);
      rs = ps.executeQuery();
      rs.next();
      int newH = rs.getInt("height") - decrH;
      sqlText = "UPDATE a2.country SET height=? WHERE cid=?";
      ps = connection.prepareStatement(sqlText);
      ps.setInt(1, newH);
      ps.setInt(2, cid); 
      ps.executeUpdate();
      ps.close(); 
      return true;
    } catch (SQLException s) {
      // s.printStackTrace();
    }
    return false;
  }
    
  public boolean updateDB() {
    try {
      sql = connection.createStatement();
      sqlText = "CREATE TABLE a2.mostPopulousCountries (cid INTEGER,cname VARCHAR(20));";
      sql.executeUpdate(sqlText);
      sqlText = "INSERT INTO a2.mostPopulousCountries SELECT cid, cname FROM country WHERE population>100000000 ORDER BY population ASC";
      sql.executeUpdate(sqlText);
      sql.close();
      return true;
    } catch (SQLException s) {
      // s.printStackTrace();
    }
    return false;
  }
  
  
  // public static void main(String args[]) throws ClassNotFoundException, SQLException {
  //   // remove this later
  //   Assignment2 a = new Assignment2();
  //   String url = "jdbc:postgresql://localhost:5432/csc343h-g4berkay";
  //   String username = "g4berkay";
  //   String password = "";
  //   a.connectDB(url, username, password);
  //   // System.out.println("abc");
  //   a.insertCountry(0, "aha", 0, 0);
  //   // System.out.println(a.chgHDI(1, 2, 1.5f));
  //   // System.out.println(a.deleteNeighbour(1, 2));
  //   // System.out.println(a.listCountryLanguages(114));
  //   // System.out.println(a.updateHeight(2, 500));
  //   // System.out.println(a.updateDB());
  //   a.disconnectDB();
  // }


}

