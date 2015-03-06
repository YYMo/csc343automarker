import java.sql.*;

public class Assignment2 {
      
  // A connection to the database  
  Connection connection;

  //CONSTRUCTOR
  Assignment2(){
    try {
      //Do some preliminary stuff
      connection = null; 
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      System.out.println("Failed to load JDBC driver");
      return;
    }
  } //done CONSTRUCTOR 

  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try {
      //get a connection  
      connection = DriverManager.getConnection(URL, username, password);
      if (connection != null)
        return true;
    } catch (SQLException e) {
      System.out.println("Failed to obtain a connection"); 
    }
    return false;
  } //dont connectDB
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try {
      // Close the connection
      connection.close();
      return true;
    } catch (SQLException e) {
       System.out.println("Failed to disconnect the connection"); 
    }
    return false;
  } //done disconnectDB
    
  public boolean insertCountry (int cid, String name, int height, int population) {
    
    // Statement to run queries
    Statement sql = null;    
    // Resultset for the query
    ResultSet rs = null;
    int rowsUpdated = 0; 

    try {
      //must check if the country already exists or not
     String query; 
     query = "SELECT * " +
             "FROM A2.country AS temp " +
             "WHERE temp.cid = " + Integer.toString(cid) + ";";
      sql = connection.createStatement();
      rs = sql.executeQuery(query);   
      //if there are any results
      if (rs.next()) {
        //the country already exists in the database
        return false; 
      }          
      //else, the country does not exist in the database
      //must add it now
      query =  "INSERT INTO A2.country VALUES " + 
               "(" + Integer.toString(cid) + ", '" + name + "', " + Integer.toString(height) + ", " +
                Integer.toString(population) + ");";  
      rowsUpdated = sql.executeUpdate(query);
      if (rowsUpdated == 1) {
        // success
        return true;
      }
    } catch (SQLException e) {
        //some sql error occured
    }
     return false;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
     // Statement to run queries
    Statement sql = null;
    // Resultset for the query
    ResultSet rs = null;

    try {
      //must check if the country already exists or not
     String query; 
     query = "SELECT COUNT(*) " +
             "FROM A2.oceanAccess " +
             "WHERE oid = " + Integer.toString(oid) + ";";  
      sql = connection.createStatement();
      rs = sql.executeQuery(query);  
      //if there are no results
      rs.next(); 
      return rs.getInt("count");
    } catch (SQLException e) {
      //
    }
	  return -1; 
  }
   
  public String getOceanInfo(int oid){
    // Statement to run queries
    Statement sql = null;
    // Resultset for the query
    ResultSet rs = null;

    try {
      //must check if the country already exists or not
     String query; 
     query = "SELECT * " +
             "FROM A2.ocean " +
             "WHERE oid = " + Integer.toString(oid) + ";";  
      sql = connection.createStatement();
      rs = sql.executeQuery(query);  
      //if there are no results
      if (!rs.next()) {
        return ""; 
      }  
      return Integer.toString(rs.getInt("oid")) + ":" + rs.getString("oname") + ":" + Integer.toString(rs.getInt("depth")); 
    } catch (SQLException e) {
      //
    }
    return "";
  }

  public boolean chgHDI(int cid, int year, float newHDI) {
   // Statement to run queries
    Statement sql = null;
    // Resultset for the query
    ResultSet rs = null;
    int rowsUpdated = 0; 

    try {
      //must check if the country already exists or not
     String query; 
     query = "SELECT * " +
             "FROM A2.hdi as temp " +
             "WHERE temp.year = " + Integer.toString(year) + " AND temp.cid = " + Integer.toString(cid) + ";";  
      sql = connection.createStatement();
      rs = sql.executeQuery(query);  
      //if there are no results
      if (!rs.next()) {
        return false; 
      }  
      //some result exists, we must update it
      query = "UPDATE A2.hdi " +
              "SET hdi_score = " + Float.toString(newHDI) + " " + 
              "WHERE year = " + Integer.toString(year) + " AND cid = " + Integer.toString(cid) + ";";
      rowsUpdated = sql.executeUpdate(query);
      if (rowsUpdated == 1) {
        // success
        return true;
      }
    } catch (SQLException e) {
      //
    }
    return false;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
     // Statement to run queries
    Statement sql = null;
    // Resultset for the query
    ResultSet rs = null;
    int rowsUpdated = 0;

    try {
     //update the entry for c1id
      sql = connection.createStatement();
      String query; 
      query = "DELETE FROM A2.neighbour " +
             "WHERE country = " + Integer.toString(c1id) + " AND neighbor = " + Integer.toString(c2id) + ";";  
      rowsUpdated = sql.executeUpdate(query); 
      if (rowsUpdated != 1) {
        return false;
      }
      query = "DELETE FROM A2.neighbour " +
             "WHERE country = " + Integer.toString(c2id) + " AND neighbor = " + Integer.toString(c1id) + ";"; 
      rowsUpdated = sql.executeUpdate(query);
      if (rowsUpdated == 1) {
        // success
        return true;
      }
    } catch (SQLException e) {
      //
    }
   return false;        
  }
  
  public String listCountryLanguages(int cid){
	  // Statement to run queries
    Statement sql = null;
    // Resultset for the query
    ResultSet rs = null;
    int rowsUpdated = 0;
    try {
     //update the entry for c1id
      sql = connection.createStatement();
      String query; 
      //get the population of the country
      query = "Select population " +
              "FROM A2.country AS temp " + 
              "WHERE temp.cid = " + Integer.toString(cid) + ";"; 
      rs = sql.executeQuery(query);  
      if (!rs.next()) {
        //the country does not exist!
        return ""; 
      }
      int population = rs.getInt("population"); 
      //figure out the languages in the country
      query = "SELECT lid, lname, lpercentage " +
             "FROM A2.language AS temp " +
             "WHERE temp.cid = " + Integer.toString(cid) + 
             " ORDER BY lpercentage;";     
      rs = sql.executeQuery(query); 
      String result = ""; 
      int len = 0; 
      while (rs.next()) {
        result = result + Integer.toString(rs.getInt("lid")) + ":" + rs.getString("lname") + ":" 
                 + Integer.toString((int) rs.getFloat("lpercentage")*population/100) + "#"; 
        len = result.length() - 1;          
      }  
      return result.substring(0, len); 
    } catch (SQLException e) {
      //
    }
    return "";
  }
  
  public boolean updateHeight(int cid, int decrH){
    // Statement to run queries
    Statement sql = null;
    // Resultset for the query
    ResultSet rs = null;
    int rowsUpdated = 0; 

    try {
     //must get the height of the country
      String query; 
      query = "SELECT * " +
             "FROM A2.country AS temp " +
             "WHERE temp.cid = " + Integer.toString(cid) + ";";   
      sql = connection.createStatement();
      rs = sql.executeQuery(query);  
      //if there are no results
      if (!rs.next()) {
        //country does not exist!
        return false; 
      }  
      int height = rs.getInt("height");
      int newHeight = height - decrH; 
      //some result exists, we must update it
      query = "UPDATE A2.country as temp " +
              "SET height = " + Integer.toString(newHeight) + " " + 
              "WHERE temp.cid = " + Integer.toString(cid) + ";";
      rowsUpdated = sql.executeUpdate(query);
      if (rowsUpdated == 1) {
        // success
        return true;
      }
    } catch (SQLException e) {
      //
    }
    return false;
  }
    
  public boolean updateDB(){
     // Statement to run queries
    Statement sql = null;
    // Resultset for the query
    ResultSet rs = null;
    int rowsUpdated = 0;  
    String query; 

    try {
     //find countries with a population over 100 mill
      sql = connection.createStatement();
      //check if the table already exists or not
      query = "SELECT * FROM A2.mostPopulousCountries;"; 
      sql.executeQuery(query);
      //the table exists
      //delete it
      query = "DROP TABLE A2.mostPopulousCountries;"; 
      sql.executeUpdate(query);  
    } catch(SQLException e) {
      //the table does not exist
      //we are good
    }
    try {
      query = "CREATE TABLE A2.mostPopulousCountries " + 
               "(cid INTEGER, " +
               "cname VARCHAR(20), " + 
               "PRIMARY KEY (cid));"; 
      sql.executeUpdate(query); 
      query = "SELECT cid, cname " +
             "FROM A2.country " +
             "WHERE population > 100000000 " +
             "ORDER BY cid ASC;";   
      rs = sql.executeQuery(query);  
      query = "INSERT INTO A2.mostPopulousCountries VALUES "; 
      while(rs.next()) {
        query = query + "(" + Integer.toString(rs.getInt("cid")) + ", '" + rs.getString("cname") + "'),";          
      }
      query = query.substring(0, query.length() - 1) + ';'; 
      if (query.length() > 45) {
        sql.executeUpdate(query);
      } 
      return true; 
    } catch (SQLException e) {
    //
    }
    return false; 
  }
}
