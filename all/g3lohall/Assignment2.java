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
      //Load JDBC Driver
      Class.forName("org.postgresql.Driver");

    } catch (ClassNotFoundException e) {

      System.out.println("Failed to find the JDBC driver"); 
      e.printStackTrace();
      return;
    }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
    
    try {
    
      connection = DriverManager.getConnection(URL, username, password);

    } catch (SQLException sq) {

      return false;
    }

    return true;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
    try {
          connection.close();

      } catch (SQLException sq) {
        
        return false;
      }

      return true;
  }
    
  //Inserts a row into the country table using the input parameters. Returns true if the insertion was successful
  public boolean insertCountry (int cid, String name, int height, int population) {

    String sqlText;
    sqlText = "INSERT INTO a2.country (cid,cname,height,population) SELECT ?,?,?,?  " + 
              "WHERE NOT EXISTS (SELECT cid FROM a2.country WHERE cid = ?)";
    
    int result;

    try{

        ps = connection.prepareStatement(sqlText);
        ps.setInt(1,cid);
        ps.setString(2,name);
        ps.setInt(3,height);
        ps.setInt(4,population);
        ps.setInt(5,cid);

        result = ps.executeUpdate();

    } catch (SQLException sq) {

      return false;
    }

    //Check for successful insertion
    if (result == 1) {
      return true;
    }
    
    //Return false upon unsuccessful insertion
    return false;

  }
  
  //Returns the number of countries in table "OceanAccess" that are located next to the ocean with id oid. Returns -1 if an error occurs
  public int getCountriesNextToOceanCount(int oid) {

    String sqlText;
    sqlText = "SELECT COUNT(cid) AS cnum FROM a2.oceanAccess " +
              "WHERE oid = ?";

    int result;
    
    try {

      ps = connection.prepareStatement(sqlText);
      ps.setInt(1, oid);
      rs = ps.executeQuery();

      result = rs.getInt("cnum");

    } catch (SQLException sq) {
      
      return -1;
    }
  
    return result;

  }
   
  //Returns a string with the information of an ocean with id oid. Returns empty string if the ocean does not exist
  public String getOceanInfo(int oid){
    
    String sqlText;
    sqlText = "SELECT oid, oname, depth FROM a2.ocean WHERE oid = ?";
    
    String result = "";

    try {

      ps = connection.prepareStatement(sqlText);
      ps.setInt(1,oid);
      rs = ps.executeQuery();

      if (rs.next()){
        result += String.valueOf(rs.getInt("oid")) + ":";
        result += rs.getString("oname")) + ":";
        result += String.valueOf(rs.getInt("depth"));
      }

    } catch (SQLException sq) {

      return "";
    }

    return result;
  }

  //Changes the HDI value. Returns true if the change was successful
  public boolean chgHDI(int cid, int year, float newHDI){
    
    String sqlText;
    sqlText = "UPDATE a2.hdi SET hdi = ? " + 
              "WHERE (cid = ? AND year = ? )";

    int result;

    try {

      ps = connection.prepareStatement(sqlText);
      ps.setFloat(1,newHDI);
      ps.setInt(2,cid);
      ps.setInt(3,year);

      result = ps.executeUpdate();

    } catch (SQLException sq) {

      return false;
    }
    
    if (result == 1) {
      return true;
    }
    
    return false;

  }
  
  //Deletes the neighboring relation between two countries. Returns true if the deletion was successful
  public boolean deleteNeighbour(int c1id, int c2id){
    String sqlText;
    sqlText = "DELETE FROM a2.neighbour WHERE ((country = ? AND neighbor = ?) OR (country =? AND neighbor = ?))";
    
    int result;

    try {

      ps = connection.prepareStatement(sqlText);
      ps.setInt(1,c1id);
      ps.setInt(2,c2id);
      ps.setInt(3,c2id);
      ps.setInt(4,c1id);

      result = ps.executeUpdate();

    } catch (SQLException sq) {

      return false;
    }  
   
    if (result == 2) {
      return true;
    }
    
    return false;

  }
  
  //Returns a string with all the languages that are spoken in the country with id cid
  public String listCountryLanguages(int cid){
	  String sqlText1;
    sqlText1 = "SELECT population FROM a2.country WHERE cid = ?";

    String sqlText2;
    sqlText2 = "SELECT lid, lname, lpercentage FROM a2.language WHERE cid = ? ORDER BY lpercentage DESC" ;

    String result = "";
    int population;
    float percentage;

    try {

      ps = connection.prepareStatement(sqlText1);
      ps.setInt(1,cid);
      rs = ps.executeQuery();
      population = rs.getInt("population");

      ps = connection.prepareStatement(sqlText2);
      ps.setInt(1,cid);
      rs = ps.executeQuery();

      while (rs.next()) {

        result += String.valueOf(rs.getInt("lid")) + ":";
        result += rs.getString("lname") + ":";

        percentage = rs.getInt("lpercentage") * population;

        result += String.valueOf(percentage) + "#";
      }

    } catch (SQLException sq) {

      return "";
    }
      
    return result;
  
  }
  
  //Decreases the height of the country with id cid. Returns true if the update was successful
  public boolean updateHeight(int cid, int decrH){

    String sqlText1;
    sqlText1 = "SELECT height FROM a2.country WHERE cid = ?";

    String sqlText2;

    int height;
    int result;

    try {

      ps = connection.prepareStatement(sqlText1);
      ps.setInt(1, cid);
      rs = ps.executeQuery();
      height = rs.getInt("height");

      if (height >= decrH) {
        height = height - decrH;

        sqlText2 = "UPDATE a2.country SET height = ? WHERE cid = ?";
        ps = connection.prepareStatement(sqlText2);
        ps.setInt(1,height);
        ps.setInt(2,cid);

        result = ps.executeUpdate();
      } else {
        return false;
      }

    } catch (SQLException sq) {
      
      return false;
    }  
   
    if (result == 1) {
      return true;
    }

    return false;

  }
    
  public boolean updateDB(){
    
  	String sqlText;
    sqlText = "CREATE TABLE IF NOT EXISTS mostPopulousCountries " +
              "AS (SELECT cid, cname FROM a2.country WHERE population > 100000000 ORDER BY cid ASC)";
    
    try {

      ps = connection.prepareStatement(sqlText);
      rs = ps.executeQuery();

    } catch (SQLException sq) {

      return false;
    }
    
  return true;    
  }
  
}
