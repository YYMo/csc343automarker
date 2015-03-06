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
      catch( ClassNotFoundException e) {
          System.out.println("Failed to find the JDBC driver.");
      }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try
      {
          connection = DriverManager.getConnection(URL,username,password);
          return true;
      }
      catch(SQLException se)
      {
          System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
          return false;
      }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try{
          if( sql != null )
              sql.close();
          if( ps != null )
              ps.close();
          if( rs != null )
              rs.close();
          if( connection != null)
              connection.close();
          return true;
      }
      catch(SQLException se)
      {
          System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
          return false;
      }
      catch(Exception e)
      {
          System.err.println("Exception." + "<Message>: " + e.getMessage());
          return false;    
      }
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
      try{
          String query = "INSERT INTO a2.country(cid,cname,height,population) VALUES (?,?,?,?)";
          ps = connection.prepareStatement(query);
          ps.setInt(1,cid);
          ps.setString(2,name);
          ps.setInt(3,height);
          ps.setInt(4,population);
          int rowCount = ps.executeUpdate();
          ps.close();
          ps = null;

          if( rowCount < 1 )
              return false;
          else
              return true;
      }
      catch(SQLException se)
      {
          if( se.getSQLState().equals("23505") )
              return false;
          else
              System.err.println("SQL Exception." +"<Code>:"+ se.getSQLState()+ "<Message>: " + se.getMessage());
          return false;
      }
      catch(Exception e)
      {
          System.err.println("Exception." + "<Message>: " + e.getMessage());
          return false;
      }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
      int result = -1;
      try{
          String query = "SELECT COUNT(*) FROM A2.oceanAccess WHERE oid = ?";
          ps = connection.prepareStatement(query);
          ps.setInt(1,oid);
          rs = ps.executeQuery();

          rs.next();
          result = rs.getInt("count");

          ps.close();
          ps = null;
          rs.close();
          rs = null;
      }
      catch(SQLException se)
      {
          System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
      }
      catch(Exception e)
      {
          System.err.println("Exception." + "<Message>: " + e.getMessage());
      }
      return result;
  }
   
  public String getOceanInfo(int oid){
      String result = "";
      try{
          String query = "SELECT oid, oname, depth FROM A2.ocean WHERE oid = ?";
          ps = connection.prepareStatement(query);
          ps.setInt(1,oid);
          rs = ps.executeQuery();

          java.lang.StringBuilder builder = new java.lang.StringBuilder();
          int count = 0;
          while( rs.next() ) {
             if( count != 0 )
                 builder.append("#");
             builder.append(rs.getInt("oid"));
             builder.append(":");
             builder.append(rs.getString("oname").trim());
             builder.append(":");
             builder.append(rs.getInt("depth"));
             count++;
          }
          result = builder.toString();
          ps.close();
          ps = null;
          rs.close();
          rs = null;
      }
      catch(SQLException se)
      {
          System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
      }
      catch(Exception e)
      {
          System.err.println("Exception." + "<Message>: " + e.getMessage());
      }
      return result;
  }

  public boolean chgHDI(int cid, int year, float newHDI){
      try{
          String query = "UPDATE A2.hdi SET hdi_score=? WHERE cid=? AND year=?";
          ps = connection.prepareStatement(query);
          ps.setFloat(1,newHDI);
          ps.setInt(2,cid);
          ps.setInt(3,year);
          int rowCount = ps.executeUpdate();
          ps.close();
          ps = null;

          if( rowCount < 1 )
              return false;
          else
              return true;
      }
      catch(SQLException se)
      {
          System.err.println("SQL Exception." +"<Code>:"+ se.getSQLState()+ "<Message>: " + se.getMessage());
          return false;
      }
      catch(Exception e)
      {
          System.err.println("Exception." + "<Message>: " + e.getMessage());
          return false;
      }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
      try{
          String query = "DELETE FROM A2.neighbour WHERE (country=? AND neighbor=?) OR (country=? AND neighbor=?)";
          ps = connection.prepareStatement(query);
          ps.setInt(1,c1id);
          ps.setInt(2,c2id);
          ps.setInt(3,c2id);
          ps.setInt(4,c1id);
          int rowCount = ps.executeUpdate();
          ps.close();
          ps = null;

          if( rowCount < 1 )
              return false;
          else
              return true;
      }
      catch(SQLException se)
      {
          System.err.println("SQL Exception." +"<Code>:"+ se.getSQLState()+ "<Message>: " + se.getMessage());
          return false;
      }
      catch(Exception e)
      {
          System.err.println("Exception." + "<Message>: " + e.getMessage());
          return false;
      }
  }
  
  public String listCountryLanguages(int cid){
      String result = "";
      try{
          String query = "SELECT lid, lname, lpercentage*population/100 as population  FROM a2.language NATURAL JOIN a2.country WHERE cid = ? ORDER BY population";
          ps = connection.prepareStatement(query);
          ps.setInt(1,cid);
          rs = ps.executeQuery();

          java.lang.StringBuilder builder = new java.lang.StringBuilder();
          int count = 0;
          while( rs.next() ) {
             if( count != 0 )
                 builder.append("#");
             builder.append(rs.getInt("lid"));
             builder.append(":");
             builder.append(rs.getString("lname").trim());
             builder.append(":");
             builder.append(rs.getInt("population"));
             count++;
          }
          result = builder.toString();
          ps.close();
          ps = null;
          rs.close();
          rs = null;
      }
      catch(SQLException se)
      {
          System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
      }
      catch(Exception e)
      {
          System.err.println("Exception." + "<Message>: " + e.getMessage());
      }
      return result;
  }
  
  public boolean updateHeight(int cid, int decrH){
      try{
          String query = "UPDATE A2.country SET height=height-? WHERE cid=?";
          ps = connection.prepareStatement(query);
          ps.setFloat(1,decrH);
          ps.setInt(2,cid);
          int rowCount = ps.executeUpdate();
          ps.close();
          ps = null;

          if( rowCount < 1 )
              return false;
          else
              return true;
      }
      catch(SQLException se)
      {
          System.err.println("SQL Exception." +"<Code>:"+ se.getSQLState()+ "<Message>: " + se.getMessage());
          return false;
      }
      catch(Exception e)
      {
          System.err.println("Exception." + "<Message>: " + e.getMessage());
          return false;
      }
  }
    
  public boolean updateDB(){
      try
      {
          sql = connection.createStatement();
          String query = "DROP TABLE IF EXISTS A2.mostPopulousCountries";
          sql.addBatch(query);
          query = "CREATE TABLE A2.mostPopulousCountries(cid INTEGER, cname VARCHAR(20))";
          sql.addBatch(query);
          query = "INSERT INTO A2.mostpopulouscountries(cid,cname) (SELECT cid,cname FROM A2.country WHERE population > 100000000 ORDER BY cid)";
          sql.addBatch(query);

          int[] count = sql.executeBatch();

          return true;
      }
      catch(SQLException se)
      {
          System.err.println("SQL Exception." +"<Code>:"+ se.getSQLState()+ "<Message>: " + se.getMessage());
          return false;
      }
      catch(Exception e)
      {
          System.err.println("Exception." + "<Message>: " + e.getMessage());
          return false;
      }
  }
  
}
