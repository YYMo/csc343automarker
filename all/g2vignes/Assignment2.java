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
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      
      try {
          connection = DriverManager.getConnection(URL, username, password);
      }
      
      catch (SQLException e) {
          System.out.println("Connection Error: "+e.getMessage());
      }
      
      return (connection != null);
   
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      
      boolean result = false;
      
      if (connection != null) {
          try {
              connection.close();
          }
          catch (SQLException e) {
              System.out.println("Close Connection Error: "+e.getMessage());
          }
      } else { 
          System.out.println("No connection was ever open.");
      }
      
      try {
          result = connection.isValid(10);
      }
      catch (SQLException e) {
      }
      
      return (result == true);
      
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
      PreparedStatement query1;
      String q1 = "SELECT cid FROM country WHERE cid=?";
      ResultSet rs1;
      int code;
      int givenCode = cid;
      
      PreparedStatement query2;
      String q2 = "INSERT INTO country VALUES (?, ?, ?, ?)";
      ResultSet rs2;
      int gheight = height;
      String gname = name;
      int gpopulation = population;
      
      int result = 0;
      
      try {
          query1 = connection.prepareStatement(q1);
          query1.setInt(1, givenCode);
          rs1 = query1.executeQuery();
          rs1.next();
          code = rs1.getInt("cid");
          if (code == cid) {
              return false; // Country with given cid already exists. 
          }
          
          else {
              query2 = connection.prepareStatement(q2);
              query2.setInt(1, givenCode);
              query2.setString(2, gname);
              query2.setInt(3, gheight);
              query2.setFloat(4, gpopulation);
              result = query1.executeUpdate();
              }
          }
      
      catch (SQLException e) {
      }
      
      return (result == 1);
  }
  
  /*Returns the number of countries in table “oceanAccess” that are located next to the
  ocean with id oid. Returns -1 if an error occurs.
  */
  public int getCountriesNextToOceanCount(int oid) {
      PreparedStatement query;
      String q1 = "SELECT count(cid) as numofcountries  FROM oceanAccess WHERE oid=?";
      ResultSet rs;
      int count;
      int oceanCode = oid;
      
      try {
          query = connection.prepareStatement(q1);
          query.setInt(1, oceanCode);
          rs = query.executeQuery();
          rs.next();
          count = rs.getInt("numofcountries");
      }
      
      catch (SQLException e) {
          return -1;
      }
      
      return count;  
  }
   
  /* Returns a string with the information of an ocean with id oid. The output is "oid:oname:depth". 
  Returns an empty string "" if the ocean does not exist. 
  */
  public String getOceanInfo(int oid){
      String oceanInfo = "";
      PreparedStatement query;
      String q1 = "SELECT oid, oname, depth FROM ocean WHERE oid=?";
      ResultSet  rs;
      int oceanCode = oid;
      int code;
      int depth;
      
      try {
          query = connection.prepareStatement(q1);
          query.setInt(1, oceanCode);
          rs = query.executeQuery();
          rs.next();
          code = rs.getInt("oid"); 
          depth = rs.getInt("depth");
          oceanInfo = oceanInfo + Integer.toString(code) + ":" + rs.getString("oname") + ":" + Integer.toString(depth);
      }
      
      catch (SQLException e) {
      }
       
   return oceanInfo;
  }

  public boolean chgHDI(int cid, int year, float newHDI){
      PreparedStatement query1;
      String q1 = "SELECT cid FROM hdi WHERE cid=?";
      String q2 = "UPDATE hdi SET hdi_score=? WHERE cid=? AND year=?";
      PreparedStatement query2;
      ResultSet rs1;
      int gcid = cid;
      int gyear = year;
      int code;
      int result = 0;
      
      try {
          query1 = connection.prepareStatement(q1);
          query1.setInt(1, gcid); 
          rs1 = query1.executeQuery();
          rs1.next();
          code = rs1.getInt("cid");
          // If the cid exists, procedd with update. 
          if (code == cid) {
              query2 = connection.prepareStatement("UPDATE hdi SET hdi_score=newHDI WHERE cid=gcid AND year=gyear");
              query2.setFloat(1, newHDI);
              query2.setInt(2, gcid);
              query2.setInt(3, gyear);
              result = query2.executeUpdate();
          }
      }
          
      catch (SQLException e) {
      }
      
      return (result == 1);
  }

  public boolean deleteNeighbour(int c1id, int c2id){
   return false;        
  }
  
  public String listCountryLanguages(int cid){
      String languages = "";
      PreparedStatement query;
      String q1 = "SELECT CL.lid, CL.lname, (CL.lpercentage/100*CO.population) "
                  + "as population FROM language CL JOIN country CO ON "
                  + "CL.cid=CO.cid WHERE CL.cid=? ORDER BY population DESC";
      ResultSet rs;
      int countryCode = cid;
      float population;
      int lid;
      String lname;
      
      try {
          query = connection.prepareStatement(q1);
          query.setInt(1, countryCode); 
          rs = query.executeQuery();
          
          int i=1;
          
          while (rs.next()) {
              String j = Integer.toString(i);
              lid = rs.getInt("lid");
              lname = rs.getString("lname");
              population = rs.getFloat("population");
              if (languages.compareTo("") == 0) {
                  languages = "|" + j + lid + ":|" + j + lname + ":|" + j + Float.toString(population); 
              } else {
                  languages = "#" + "|" + j + lid + ":|" + j + lname + ":|" + j + Float.toString(population); 
              }
              i = i +1;
          }
      }
      
      catch (SQLException e) {
      }
      
      return languages;
  }
  
  public boolean updateHeight(int cid, int decrH){
      PreparedStatement query1;
      String q1 = "SELECT cid, height FROM country WHERE cid=?";
      String q2 = "UPDATE country SET height=? WHERE cid=?";
      PreparedStatement query2;
      ResultSet rs1;
      int gcid = cid;
      int height;
      int code;
      int result = 0;
      
      try {
          query1 = connection.prepareStatement(q1);
          query1.setInt(1, gcid); 
          rs1 = query1.executeQuery();
          rs1.next();
          code = rs1.getInt("cid");
          height = rs1.getInt("height");
          // If the cid exists, proceed with update. 
          if (code == cid) {
              int newHeight = height - decrH;
              query2 = connection.prepareStatement(q2);
              query2.setInt(1, newHeight); 
              query2.setInt(2, gcid); 
              result = query2.executeUpdate();
          }
      }
          
      catch (SQLException e) {
      }
      
      return (result == 1);
  }
    
  public boolean updateDB(){
	return false;    
  }
  
}