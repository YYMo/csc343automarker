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
        connection = null;

        Class.forName("org.postgresql.Driver");
      }
      catch (ClassNotFoundException e) {
        System.out.println("Failed to find the JDBC driver");
        System.exit(1);
      }

  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try{
        connection = DriverManager.getConnection(URL, username, password);
      }
      catch(SQLException e){
        System.err.println("SQL Exception in connectDB()."+
            " <Message>: " + e.getMessage());
      }
      if ( connection != null )
      {
          // System.out.println("Connection succeded.");
          return true;
      }
      return true;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try {
        connection.close();
        return true;
      }
      catch(SQLException e){
        System.err.println("SQL Exception in disconnectDB()."+
            " <Message>: " + e.getMessage());
    }
      return false;
  }

  /*
  Inserts a row into the country table. cid is the name of the country, name
  is the name of the country, height is the highest elevation point and
  population is the population of the newly inserted country. 
  You have to check if the country with id cid exists. Returns true if the 
  insertion was successful, false otherwise. */
  public boolean insertCountry (int cid, String name, int height, int population) {
    try {
      String insertTableSQL = "INSERT INTO a2.country"
      + "(cid, cname, height, population) VALUES"

      + "(?,?,?,?)";
      
      // Check if the country with id cid exists.
      sql = connection.createStatement();
      rs = sql.executeQuery("SELECT cid FROM a2.country where cid = '" + cid + "'");
      if (rs.next()) {
        if (sql != null) sql.close();
        return false; 
      } else {
        ps = connection.prepareStatement(insertTableSQL);
        ps.setInt(1, cid);
        ps.setString(2, name);
        ps.setInt(3, height);
        ps.setInt(4, population);
        ps.executeUpdate();
        if (ps != null) ps.close();
        if (sql != null) sql.close();
        return true;
      }
    } catch(SQLException e) {
      System.err.println("SQL Exception in insertCountry." + e.getMessage());
    } 
    return false; 
  }
  
  /*Returns the number of countries in table oceanAccess that are located next to
    the ocean with id oid. Returns -1 if an error occurs.*/
  public int getCountriesNextToOceanCount(int oid) {
        int count = 0;
        try{
          sql = connection.createStatement();
	        rs = sql.executeQuery("SELECT cid FROM a2.oceanAccess where oid = '" + oid + "'");
          
          while(rs.next())                                                                                                                                                                                                              
            count++;
          
          if (sql != null) sql.close();
        }
        catch(SQLException e){
          System.err.println("SQL Exception in getCountriesNextToOceanCount." + e.getMessage());
          return -1;
        }
        return count;
  }
  
  /*Returns a string with the information of an ocean with id oid. The output is
  oid:oname:depth. Returns an empty string if the ocean does not exist.*/
  public String getOceanInfo(int oid){
    String oceanInfo = ""; 
    try{
        sql = connection.createStatement();
        rs = sql.executeQuery("SELECT * FROM a2.ocean where oid = '" + oid + "'");
        if(rs.next())
          oceanInfo = rs.getString("oid") + ":" + rs.getString("oname") + ":" 
          + rs.getString("depth");

        if (sql != null) sql.close();
    }
    catch(SQLException e){
               System.err.println("SQL Exception in getOceanInfo." + e.getMessage());

    }
    return oceanInfo;
  }

  /*Changes the HDI value of the country cid for the year year to the HDI value
  supplied (newHDI). Returns true if the change was successful, false otherwise.*/
  public boolean chgHDI(int cid, int year, float newHDI){
   try {
      String updateTableSQL = "UPDATE a2.hdi SET hdi_score = ? WHERE cid = " + cid 
        + "AND year = " + year;
      
      ps = connection.prepareStatement(updateTableSQL);
      ps.setFloat(1, newHDI);
      ps.executeUpdate();

      if (ps != null) ps.close();
      return true;
   }catch(SQLException e) {
      System.err.println("SQL Exception in chgHDI." + e.getMessage());
   } 
    return false; 
  }

  /*Deletes the neighboring relation between two countries. Returns true
  if the deletion was successful, false otherwise. You can assume that the neighboring
  relation to be deleted exists in the database. Remember that if c2 is a neighbor of
  c1, c1 is also a neighbour of c2.*/
  public boolean deleteNeighbour(int c1id, int c2id){
     try {
      String deleteTableSQL = "DELETE FROM a2.neighbour WHERE (country =" + c1id + 
      "AND neighbor = " + c2id + ") OR (country =" + c2id + "AND neighbor = " + c2id +")";
      
      ps = connection.prepareStatement(deleteTableSQL);
      ps.executeUpdate();

      if (ps != null) ps.close();
      return true;

   }catch(SQLException e) {
      System.err.println("SQL Exception in deleteNeighbour." + e.getMessage());
   } 
    return false;       
  }
  
  /*Returns a string with all the languages that are spoken in the country with id
  cid. The list of languages should follow the contiguous format described above, and
  contain the following attributes in the order shown: (NOTE: before creating the
    string order your results by population).*/
  public String listCountryLanguages(int cid){
	 String countryLanguages = ""; 
    try{
        sql = connection.createStatement();
        rs = sql.executeQuery("SELECT lid, lname, ((population * lpercentage) / 100) AS popl FROM a2.language" + 
          " JOIN a2.country ON a2.language.cid = a2.country.cid WHERE country.cid =" + cid);

        for (int i = 0; rs.next(); i++){
          if (i > 0)
            countryLanguages += "#";

          countryLanguages += rs.getString("lid") + ":" + rs.getString("lname") + ":" + rs.getString("popl");
        }
        if (sql != null) sql.close();
        return countryLanguages;
    }
    catch(SQLException e){
               System.err.println("SQL Exception in listCountryLanguages." + e.getMessage());
    }
    return countryLanguages;
  }
  
  /*Decreases the height of the country with id cid. (A decrease might happen due to natural erosion.) Returns
  true if the update was successful, false otherwise.*/
  public boolean updateHeight(int cid, int decrH){ 
    try {
      int height = 0;
      sql = connection.createStatement();
      rs = sql.executeQuery("SELECT * FROM a2.country where cid = " + cid);
      // System.out.println ("")

      if(rs.next())
        height = rs.getInt("height");
      if (sql != null) sql.close();

      String updateTableSQL = "UPDATE a2.country SET height = ? WHERE cid = " + cid;
      
      ps = connection.prepareStatement(updateTableSQL);
      ps.setInt(1, height - decrH);
      ps.executeUpdate();

      if (ps != null) ps.close();
      return true;
   }catch(SQLException e) {
      System.err.println("SQL Exception in updateHeight." + e.getMessage());
   } 
    return false; 
  }

  /*Create a table containing all the countries which have a population over 100 million. */
  public boolean updateDB(){
    try
    {
      int numRow;

      String dropTableSQL = "DROP TABLE IF EXISTS a2.mostPopulousCountries CASCADE";
      String createTableSQL = "CREATE TABLE a2.mostPopulousCountries(" 
        + "cid INTEGER NOT NULL, "
        + "cname VARCHAR(20) NOT NULL, "
        + "PRIMARY KEY (cid) "
        + ")";


      sql = connection.createStatement();
      sql.executeUpdate(dropTableSQL);
      sql.executeUpdate(createTableSQL);

      String insertTableSQL = "INSERT INTO a2.mostPopulousCountries (cid, cname) (SELECT cid, cname " +
        "FROM a2.country where population > 100000000 ORDER BY cid ASC)";
      // String insertTableSQL = "INSERT INTO a2.mostPopulousCountries (cid, cname)" + rs;
      numRow = sql.executeUpdate(insertTableSQL);

      if (numRow > 0){
        sql.close();
        return true;
      }else
        return false;
    }catch(SQLException e) {
      System.err.println("SQL Exception in updateDB." + e.getMessage());
   } 
    return false; 
  }
}
