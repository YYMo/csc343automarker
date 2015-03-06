import java.sql.*;
import java.io.*;

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
  public boolean connectionDB(String URL, String username, String password){
      try{
          connection = DriverManager.getConnection(URL, username, password);
          System.out.println("Connection success!");
          return true;
      }
      catch (SQLException e){
      }
      return false;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try{
          if (connection != null){
              connection.close();
              if (connection.isClosed()){
                  return true;
              }
          }
      }
      catch (SQLException e){
      }
      return false;
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
      try{
          String insertString = "INSERT IGNORE INTO a2.country" + "(cid, cname, height, population) VALUES" + "(?, ?, ?, ?)";
          ps = connection.prepareStatement(insertString);
          ps.setInt(1, cid);
          ps.setString(2, name);
          ps.setInt(3, height);
          ps.setInt(4, population);
          ps.executeUpdate();
          return true;
      }
      catch(SQLException se){
      }
      finally{
          try{
              if (ps != null){
                  ps.close();
              }
          }
          catch (SQLException e){
          };
          try{
              if (rs != null){
                  rs.close();
              }
          }
          catch (SQLException e){
          };
      }
      return false;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
      try{
          String queryString = "select count(cid) from a2.oceanaccess where oid = ? group by oid";
          ps = connection.prepareStatement(queryString);
          ps.setInt(1, oid);
          rs = ps.executeQuery();
          int numcountries = rs.getInt("oid");
          return numcountries;
      }
      catch(SQLException se){
      }
      finally{
          try{
              if (ps != null){
                  ps.close();
              }
          }
          catch (SQLException e){
          };
          try{
              if (rs != null){
                  rs.close();
              }
          }
          catch (SQLException e){
          };
      }

	return -1;
  }
   
  public String getOceanInfo(int oid){
      try{
          String queryString = "select oid, oname, depth from a2.ocean where oid = ?";
          ps = connection.prepareStatement(queryString);
          ps.setInt(1, oid);
          rs = ps.executeQuery();
          int oidRS = rs.getInt("oid");
          String onameRS = rs.getString("oname");
          int depthRS = rs.getInt("depth");
          String oceanInfo = oidRS + ":" + onameRS + ":" + depthRS;
          return oceanInfo;
      }
      catch(SQLException se){
      }
      finally{
          try{
              if (ps != null){
                  ps.close();
              }
          }
          catch (SQLException e){
          };
          try{
              if (rs != null){
                  rs.close();
              }
          }
          catch (SQLException e){
          };
      }
      
   return "";
  }

  public boolean chgHDI(int cid, int year, float newHDI){
      try{
          String updateString = "UPDATE a2.hdi SET hdi = ? WHERE cid = ? and year = ?";
          ps = connection.prepareStatement(updateString);
          ps.setFloat(1, newHDI);
          ps.setInt(2, cid);
          ps.setInt(3, year);
          ps.executeUpdate();
          return true;
      }
      catch(SQLException se){
          se.printStackTrace();
      }
      finally{
          try{
              if (ps != null){
                  ps.close();
              }
          }
          catch (SQLException e){
          };
      }
      
   return false;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
      try{
          String deleteString = "DELETE FROM a2.neighbour WHERE EXISTS (select t1.country, t1.neighbor from a2.neighbour t1 INNER JOIN a2.neighbour t2 ON t1.country = t2.neighbor and t1.neighbor = t2.country where t1.country = ? or t1.country = ?;)";
          ps = connection.prepareStatement(deleteString);
          ps.setInt(1, c1id);
          ps.setInt(2, c2id);
          ps.executeUpdate();
          return true;
      }
      catch(SQLException se){
      }
      finally{
          try{
              if (ps != null){
                  ps.close();
              }
          }
          catch (SQLException e){
          };
      }
   return false;
  }
  
  public String listCountryLanguages(int cid){
      try{
          String lastrow = new String();
          String languageList = new String();
          String totalList = new String();
          String queryString = "select lid, lname, sum(population*lpercentage/100) as lpopulation from a2.language natural join a2.country where cid = ? group by lid, lname order by lpopulation";
          ps = connection.prepareStatement(queryString);
          ps.setInt(1, cid);
          rs = ps.executeQuery();
          while (rs.next()){
              int lid = rs.getInt("lid");
              String lname = rs.getString("lname");
              int population = rs.getInt("lpopulation");
              if (rs.last()){
                  lastrow = Integer.toString(lid) + ":" + lname + ":" + Integer.toString(population);
              }
              languageList = Integer.toString(lid) + ":" + lname + ":" + Integer.toString(population) + "#";
              totalList = languageList + lastrow;
          }
          return totalList;
      }
      catch(SQLException se){
      }
      finally{
          try{
              if (ps != null){
                  ps.close();
              }
          }
          catch (SQLException e){
          };
          try{
              if (rs != null){
                  rs.close();
              }
          }
          catch (SQLException e){
          };
      }
	return "";
  }
  
  public boolean updateHeight(int cid, int decrH){
      try{
          String updateString = "UPDATE a2.country SET height = height - ? WHERE cid = ?";
          ps = connection.prepareStatement(updateString);
          ps.setInt(1, decrH);
          ps.setInt(2, cid);
          ps.executeUpdate();
          return true;
      }
      catch(SQLException se){
      }
      finally{
          try{
              if (ps != null){
                  ps.close();
              }
          }
          catch (SQLException e){
          };
      }

    return false;
  }
    
  public boolean updateDB(){
      try{
          sql = connection.createStatement();
          String createTable = "CREATE TABLE mostPopulousCountries (cid INTEGER REFERENCES country(cid) ON DELETE RESTRICT, cname VCHAR(20) NOT NULL) AS (select cid, cname from a2.country where population > 100000000 order by cid)";
          sql.executeUpdate(createTable);
          return true;
      }
      catch(SQLException se){
      }
      finally{
          try{
              if (sql != null){
                  sql.close();
              }
          }
          catch (SQLException e){
          };
      }
	return false;    
  }
}
