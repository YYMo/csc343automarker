import java.sql.*;
import java.io.*;

public class Assignment2 {
  
  public static void main(String[] args) {
       Assignment2 test = new Assignment2();
       test.connectDB("localhost:5432/csc343h-g3mavrix", "g3mavrix", "Aidzzz1925.");
       test.insertCountry(4, "France", 40, 6000);
       test.getCountriesNextToOceanCount(2);
       test.getOceanInfo(1);
       test.chgHDI(1, 2010, 9.5f);
       test.deleteNeighbour(1, 2);
       test.updateHeight(1, 10);
       test.listCountryLanguages(1);
       test.updateDB();
  }
  
  // A connection to the database  
  Connection connection;
  
  // Statement to run queries
  Statement sql;
  
  // Prepared Statement
  PreparedStatement ps;
  
  // Resultset for the query
  ResultSet rs;
  
  // String for writing sql statements
  String sqlString;
  
  //CONSTRUCTOR
  Assignment2(){
      try 
      {
          Class.forName("org.postgresql.Driver");
      }
      catch (ClassNotFoundException e) 
      {
          System.out.println("Failed to find the JDBC driver");
      }
   }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try 
      {
          connection = DriverManager.getConnection("jdbc:postgresql://" + URL, username, password);
          return true;
      }
      catch (SQLException e)
      {
          return false;
      }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try
      {
          connection.close();
          return true;
      }
      catch (SQLException e)
      {
          return false;
      }    
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
      sqlString = "INSERT INTO a2.country VALUES" +
                  "(?, ?, ?, ?)";
      
      try 
      {
          ps = connection.prepareStatement(sqlString);
          ps.setInt(1, cid);
          ps.setString(2, name);
          ps.setInt(3, height);
          ps.setInt(4, population);
          ps.executeUpdate();
          ps.close();
          return true;
      }
      catch (SQLException e) 
      {
          return false;
      }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	sqlString = "select count(DISTINCT cid) as count from a2.oceanAccess where oid = ?";
        
        try
        {
            ps = connection.prepareStatement(sqlString);
            ps.setInt(1, oid);
            rs = ps.executeQuery();
            rs.next();
            int i = rs.getInt("count");
            rs.close();
            ps.close();

            return i; 
        }
        catch (SQLException e)
        {
          return -1;
        }
  }
   
  public String getOceanInfo(int oid){
      sqlString = "select * from a2.ocean where oid = ?";

      try
      {
          ps = connection.prepareStatement(sqlString);
          ps.setInt(1, oid);
          rs = ps.executeQuery();
          rs.next();
          int rsoid = rs.getInt("oid");
          String rsoname = rs.getString("oname");
          int rsdepth = rs.getInt("depth");
          rs.close();
          ps.close();          

          return ""+rsoid+":"+rsoname+":"+rsdepth+"";
      }
      catch (SQLException e)
      {
          return "";
      }
  }

  public boolean chgHDI(int cid, int year, float newHDI){
      sqlString = "update a2.hdi set hdi_score = ? where cid = ? and year = ?";

      try
      {
          ps = connection.prepareStatement(sqlString);
          ps.setFloat(1, newHDI);
          ps.setInt(2, cid);
          ps.setInt(3, year);
          ps.executeUpdate();
          ps.close();
          return true;
      }
      catch (SQLException e)
      {
          return false;
      }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
      sqlString = "delete from a2.neighbour where (country = ? and neighbor = ?) or (country = ? and neighbor = ?)";

      try
      {
          ps = connection.prepareStatement(sqlString);
          ps.setInt(1, c1id);
          ps.setInt(2, c2id);
          ps.setInt(3, c2id);
          ps.setInt(4, c1id);
          ps.executeUpdate();
          ps.close();
          return true;
      }
      catch (SQLException e)
      {
          return false;
      }        
  }
  
  public String listCountryLanguages(int cid) {
      sqlString = "select lid, lname, population * lpercentage / 100 as population " +
                  "from a2.language natural join a2.country where cid = ? order by population";
      StringBuilder sb = new StringBuilder();

      try
      {
          ps = connection.prepareStatement(sqlString);
          ps.setInt(1, cid);
          rs = ps.executeQuery();

          while(rs.next()) {    
              int lid = rs.getInt("lid");
              String cname = rs.getString("lname");
              int population = rs.getInt("population"); 
              sb.append("").append(lid).append(":").append(cname).append(":").append(population).append("#");
          }
          
          // get rid of last #
          sb.setLength(sb.length() - 1);

          String rString = sb.toString();
          rs.close();
          ps.close();

          return rString;
      }
      catch (SQLException e)
      {
          return "";
      }
  }
  
  public boolean updateHeight(int cid, int decrH){
      sqlString = "update a2.country set height = height - ? where cid = ?";

      try
      {
          ps = connection.prepareStatement(sqlString);
          ps.setInt(1, decrH);
          ps.setInt(2, cid);
          ps.executeUpdate();
          ps.close();
          return true;
      }
      catch (SQLException e)
      {
          return false;
      }
  }
    
  public boolean updateDB(){
      sqlString = "create table a2.mostPopulousCountries ( cid int, cname varchar(20) )";

      try
      {
          ps = connection.prepareStatement(sqlString);
          ps.executeUpdate();
          ps.close();

          sqlString = "insert into a2.mostPopulousCountries (select cid, cname from a2.country where population > 100000000)";
          ps = connection.prepareStatement(sqlString);
          ps.executeUpdate();
          ps.close();
          return true;
      }
      catch (SQLException e)
      {
          return false;
      }   
  }
  
}
