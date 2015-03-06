/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package assignment2;
import java.sql.*;

/**
 *
 * @author xuepeng1
 */
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
      }
      catch (ClassNotFoundException e) {
          
      }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try {
          connection = DriverManager.getConnection(URL, username, password);
      }
      catch(SQLException e){
          return false;
      }
      if (connection != null){
          return true;
      } else {
          return false;
      }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      if (connection == null) {
          return false;
      } else {
          try {
              connection.close();
              return true;
          }
          catch(SQLException e){
            return false;
          }
      }   
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
      try {
        String queryStr = "SELECT * FROM a2.country WHERE cid = ?;";
        ps = connection.prepareStatement(queryStr);
        ps.setInt(1, cid);
        rs = ps.executeQuery();
        if (rs != null) {
          return false;
        } else {
          queryStr = "INSERT INTO a2.country(cid, cname, height, population) VALUES (?, ?, ?, ?);";
          ps = connection.prepareStatement(queryStr);
          ps.setInt(1, cid);
          ps.setString(2, name);
          ps.setInt(3, height);
          ps.setInt(4, population);
          ps.executeUpdate();
          queryStr = "SELECT * FROM a2.country WHERE cid = ?;";
          ps = connection.prepareStatement(queryStr);
          ps.setInt(1, cid);
          rs = ps.executeQuery();
          if (rs != null) {
              return true;
          } else {
              return false;
          }
      }
      
    } catch (SQLException e) {
      return false;
    }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
      try {
        String queryStr = "SELECT count(*) AS numCountry FROM a2.oceanAccess WHERE oid = ?;";
        ps = connection.prepareStatement(queryStr);
        ps.setInt(1, oid);
        rs = ps.executeQuery();
        if (rs != null) {
                  return rs.getInt("numCountry");
        } else {
              return -1;
        }
      }
      catch (SQLException e) {
          return -1;
      }  
  }
   
  public String getOceanInfo(int oid){
      try {
        String queryStr = "SELECT * FROM a2.ocean WHERE oid = ?;";
        ps = connection.prepareStatement(queryStr);
        ps.setInt(1, oid);
        rs = ps.executeQuery();
        if (rs != null) {
            return rs.getInt("oid") + ":" + rs.getString("oname") + ":" + rs.getInt("depth");
        } else {
              return "";
        }
      }
      catch (SQLException e) {
          return "";
      }  
  }

  public boolean chgHDI(int cid, int year, float newHDI){
      try {
        String queryStr = "SELECT * FROM a2.hdi WHERE cid = ?;";
        ps = connection.prepareStatement(queryStr);
        ps.setInt(1, cid);
        rs = ps.executeQuery();
        if (rs == null) {
          return false;
        } else {
            queryStr = "UPDATE a2.hdi SET hdi = ? WHERE cdi = ? AND year = ?;";
            ps = connection.prepareStatement(queryStr);
            ps.setFloat(1, newHDI);
            ps.setInt(2, cid);
            ps.setInt(3, year);
            ps.executeUpdate();
            return true;
        }
      }
      catch (SQLException e) {
          return false;
      } 
  }

  public boolean deleteNeighbour(int c1id, int c2id){
        try {
        String queryStr = "REMOVE FROM a2.neighbour WHERE (country = ? AND neighbor = ?);";
        ps = connection.prepareStatement(queryStr);
        ps.setInt(1, c1id);
        ps.setInt(2, c2id);
	ps.executeUpdate();
        queryStr = "REMOVE FROM a2.neighbour WHERE (country = ? AND neighbor = ?);";
        ps = connection.prepareStatement(queryStr);
        ps.setInt(1, c2id);
        ps.setInt(2, c1id);
        ps.executeUpdate();
        return true;
      }
      catch (SQLException e) {
          return false;
      }        
  }
  
  public String listCountryLanguages(int cid){
      try {
        String queryStr = "SELECT l.lid, l.lname, (l.percentage * c.population) AS population FROM a2.language l, a2.country c WHERE l.cid = c.cid AND l.oid = ? ORDER BY population ASC;"; 
        // Assume "before creating the string order your results by population means ascending order
        ps = connection.prepareStatement(queryStr);
        ps.setInt(1, cid);
        rs = ps.executeQuery();
        if (rs != null) {
            String answer = "";
            while (rs.next()) {
                answer = answer + rs.getInt("lid") + ":" + rs.getString("lname") + ":" + rs.getInt("population") + "#";
            }
            return answer.substring(0, answer.length()-1);
        } else {
              return "";
        }
      }
      catch (SQLException e) {
          return "";
      }
  }
  
  public boolean updateHeight(int cid, int decrH){
      try {
        String queryStr = "SELECT * FROM a2.country WHERE cid = ?;";
        ps = connection.prepareStatement(queryStr);
        ps.setInt(1, cid);
        rs = ps.executeQuery();
        if (rs == null) {
          return false;
        } else {
            queryStr = "UPDATE a2.country SET height = height - ? WHERE cdi = ?;";
            ps = connection.prepareStatement(queryStr);
            ps.setInt(1, decrH);
            ps.setInt(2, cid);
            ps.executeUpdate();
            return true;
        }
      }
      catch (SQLException e) {
          return false;
      }
  }
    
  public boolean updateDB(){
      try {
        String queryStr = "CREATE TABLE a2.mostPopulousCountries ( cid  INTEGER, cname VARCHAR(20));";
        ps = connection.prepareStatement(queryStr);
        rs = ps.executeQuery();
	queryStr = "SELECT cid, cname FROM country WHERE population > 100000000 ORDER BY cid ASC;";
        ps = connection.prepareStatement(queryStr);
        rs = ps.executeQuery();
	if (rs == null) {
		return false;
	}
	else {
		while (rs.next()) {
			queryStr = "INSERT INTO a2.mostPopulousCountries(cid, cname) VALUES (?, ?,);";
			ps = connection.prepareStatement(queryStr);
          		ps.setInt(1, rs.getInt("cid"));
          		ps.setString(2, rs.getString("cname"));
          		ps.executeUpdate();
		}

	}
        return true;
      }
      catch (SQLException e) {
          return false;
      }    
  }
  
}

  
