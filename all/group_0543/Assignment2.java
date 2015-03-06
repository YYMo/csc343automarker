import java.sql.*;
import java.io.*;

public class Assignment2 {
    
  // A connection to the database  
  private Connection connection;
  
  //CONSTRUCTOR
  /**
   * Identifies the postgreSQL driver using Class.forName method. 
   */
  public Assignment2(){
      try {
          Class.forName("org.postgresql.Driver");
      } catch (ClassNotFoundException e) {
          System.out.println("Failed to find the JDBC driver");
      }
  }
  
  /**
   * Using the String input parameters which are the URL, username, 
   * and password respectively, establish the Connection to be used for this 
   * session.
   * 
   * @param URL
   * @param username
   * @param password
   * @return true if the connection was successful, false otherwise
   */
  public boolean connectDB(String URL, String username, String password){
      try {
          connection = DriverManager.getConnection(URL, username, password);
      } catch (SQLException se) {
    	  
          return false;
      }
      return true;
  }
  
  /**
   * Closes the connection.
   * @return true if the closure was successful, false otherwise
   */
  public boolean disconnectDB(){
      try {
          connection.close();
      } catch (Exception e) {
          
          return false;
      }
      return true;
  }
  
  /**
   * Inserts a row into the country table.
   * 
   * @param cid: the name of the country
   * @param name: name of the country
   * @param height: the highest elevation point
   * @param population: population of the newly inserted country
   * @return true if the insertion was successful, false otherwise.
   */
  public boolean insertCountry (int cid, String name, int height, int population) {
      try {
          PreparedStatement newCountry = connection.prepareStatement(
                "INSERT INTO a2.country(cid, cname, height, population) "
                + "VALUES(?, ?, ?, ?)");
          
          // pass the parameters to the query
          newCountry.setInt(1, cid);
          newCountry.setString(2, name);
          newCountry.setInt(3, height);
          newCountry.setInt(4, population);        
          newCountry.executeUpdate();
          // close the statement
          newCountry.close();
          
      } catch (SQLException se) {
          
          return false;
      }
      return true;
  }
  
  /**
   * Returns the number of countries in table oceanAccess that are located 
   * next to the ocean with id oid.
   * 
   * @param oid: id of the ocean
   * @return the number of countries, -1 if the ocean with oid does not exist
   */
  public int getCountriesNextToOceanCount(int oid) {
      try {
          PreparedStatement countCountries = connection.prepareStatement(
             "SELECT COUNT(*) FROM a2.oceanAccess WHERE oid = ?");
          
          countCountries.setInt(1, oid);
          ResultSet countSet = countCountries.executeQuery();
          
          int count = 0;	  
          if (countSet.next()) {
              count = countSet.getInt(1);
          }
          // close the statement and the result set
          countCountries.close();
          countSet.close();
          
          return count;
          
      } catch (SQLException se) {
          
          return -1;
      }
  }
  
  /**
   * Returns a string with the information of an ocean.
   * 
   * @param oid: id of the ocean
   * @return the information string, empty string if the ocean does not exist.
   */
  public String getOceanInfo(int oid){
      try {
          PreparedStatement oceanInfo = connection.prepareStatement(
              "SELECT * FROM a2.ocean WHERE oid = ?");
          
          oceanInfo.setInt(1, oid);
          ResultSet infoSet = oceanInfo.executeQuery();
          
          String info = "";
          if (infoSet.next()) {
              info = Integer.toString(infoSet.getInt(1)).trim() 
            		  + ":"+ infoSet.getString(2).trim() + ":"+
            		  Integer.toString(infoSet.getInt(3)).trim();
          }
          
          oceanInfo.close();
          infoSet.close();
          
          return info;
          
      } catch (SQLException se) {
          
          return "";
      }
  }
  
  /**
   * Changes the HDI value of the country cid for the year year to the 
   * HDI value supplied (newHDI). 
   * 
   * @param cid: id of the country
   * @param year: the year that need to be changed
   * @param newHDI: the new HDI value
   * @return true if the change was successful, false otherwise.
   */
  public boolean chgHDI(int cid, int year, float newHDI){
	  try {
          PreparedStatement updateHDI = connection.prepareStatement(
                "UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? AND year = ?");
          
          updateHDI.setFloat(1, newHDI);
          updateHDI.setInt(2, cid);
          updateHDI.setInt(3, year);
          int updateResult = updateHDI.executeUpdate();
          updateHDI.close();
          // if the executeUpdate() return 0, it means nothing got updated
          if (updateResult == 0){
        	  return false;
          } 
          
      } catch (SQLException se) {
          
          return false;
      }
      return true;
  }
  
  /**
   * Deletes the neighboring relation between two countries.
   * 
   * @param c1id: id of the first country
   * @param c2id: id if the second country
   * @return true if the deletion was successful, false otherwise. 
   */
  public boolean deleteNeighbour(int c1id, int c2id){
	  try {
          PreparedStatement deleteNeighbor1 = connection.prepareStatement(
                "DELETE FROM a2.neighbour WHERE country = ? AND neighbor = ?");
          
          deleteNeighbor1.setInt(1, c1id);
          deleteNeighbor1.setInt(2, c2id);
          int updateResult1 = deleteNeighbor1.executeUpdate();
          
          // need to delete the other pair as well
          PreparedStatement deleteNeighbor2 = connection.prepareStatement(
                "DELETE FROM a2.neighbour WHERE country = ? AND neighbor = ?");
            
          deleteNeighbor2.setInt(1, c2id);
          deleteNeighbor2.setInt(2, c1id);        
          int updateResult2 = deleteNeighbor2.executeUpdate();
          
          deleteNeighbor1.close();
          deleteNeighbor2.close();
          
          if (updateResult1 == 1 && updateResult2 == 1) {
        	  return true;
          } else {
        	  return false;
          }
          
      } catch (SQLException se) {
          
          return false;
      }     
  }
  
  /**
   * Find all the languages that are spoken in the country with id cid, with 
   * the population that speaks that language, ordered by population in 
   * ascending order 
   * 
   * @param cid: id of the country
   * @return the string of all the languages that are spoken in the country
   */
  public String listCountryLanguages(int cid){
	  try {
          PreparedStatement forCountryLanguages = connection.prepareStatement(
              "SELECT lid, lname, lpercentage*(SELECT population "
              + "FROM a2.country WHERE cid = ?) AS lpopulation "
              + "FROM a2.language "
              + "WHERE cid = ? "
              + "ORDER BY lpopulation");
          
          forCountryLanguages.setInt(1, cid);
          forCountryLanguages.setInt(2, cid);
          ResultSet languageSet = forCountryLanguages.executeQuery();
          
          String languages = "";
          while (languageSet.next()) {
        	  if (! languages.equals("")){
        		  languages += "#";
        	  }
        	  languages += Integer.toString(languageSet.getInt(1)).trim() 
            		  + ":"+ languageSet.getString(2).trim() + ":"+
            		  Float.toString(languageSet.getFloat(3)).trim();
          }
          
          forCountryLanguages.close();
          languageSet.close();
          
          return languages;
          
      } catch (SQLException se) {
          
          return "";
      }
  }
  
  /**
   * Decreases the height of the country with id cid. 
   * 
   * @param cid: id of the country
   * @param decrH: the height to decrease
   * @return  true if the update was successful, false otherwise.
   */
  public boolean updateHeight(int cid, int decrH){
	  try {
          PreparedStatement decreaseHeight = connection.prepareStatement(
             "UPDATE a2.country SET height "
             + "= ((SELECT height FROM a2.country WHERE cid = ?) - ?) "
             + "WHERE cid = ?");
          
          decreaseHeight.setInt(1, cid);
          decreaseHeight.setInt(2, decrH);
          decreaseHeight.setInt(3, cid);
          int updateResult = decreaseHeight.executeUpdate(); 
          decreaseHeight.close();
          
          if (updateResult == 0){
        	  return false;
          }         
          
      } catch (SQLException se) {
          
          return false;
      }
      return true;
  }
  
  /**
   * Create a table mostPopulousCountries with attributes cid and cname, 
   * containing all the countries which have a population over 100 million, 
   * ordered by cid in ascending order. 
   * 
   * @return true if the database was successfully updated, false otherwise. 
   */
  public boolean updateDB(){
	  try {
          PreparedStatement newtable = connection.prepareStatement(
             "CREATE TABLE a2.mostPopulousCountries AS "
             + "SELECT cid, cname "
             + "FROM a2.country "
             + "WHERE population > 100000000 "
             + "ORDER BY cid ASC");
      
          newtable.executeUpdate();
          newtable.close();
          
      } catch (SQLException se) {
          
          return false;
      }
      return true; 
  }
  
}
