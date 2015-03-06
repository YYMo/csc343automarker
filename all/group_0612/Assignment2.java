/*
 * CSC343: ASSIGNMENT 2, PART 2
 * NOVEMBER 10, 2014
 * g3luke & c4vantha
 */

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
          Class.forName("org.postgresql.Driver");
      } catch(ClassNotFoundException e){
          e.printStackTrace();
      }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try {
          connection = DriverManager.getConnection(URL, username, password);
          
          // Create a statement
          sql = connection.createStatement();
          
          return true;
      } catch(SQLException e) {
          return false;
      }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try {
          connection.close();
          sql.close();
          
          // Also closes the rs if it's being used:
          if(rs!=null){
              rs.close();
          }
          
          return true;
      } catch(SQLException e) {
          return false;
      }    
  }
    
  
  public boolean insertCountry (int cid, String name, int height, int population) {
      try {
          // First check if there already exists a country with this cid
          rs = sql.executeQuery("SELECT cid FROM country WHERE cid = " + cid);
          
          if(rs.next()){    // a country w/ this cid already exists in the DB!
              disconnectDB();
              rs.close();
              return false;
          }
          
          rs.close();
          
          // Perform the insertion!
          rs = sql.executeQuery("INSERT INTO country(cid,cname,height,population) VALUES ('" +
                                    cid +"', '" + name + "', '" + height + "', '" + population + "')");
          rs.close();
          return true;                          
                                    
      } catch (SQLException e) {
          e.printStackTrace();
          disconnectDB();   // should it?
          return false;
      }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
    return -1;  
  }
   
  public String getOceanInfo(int oid){
   return "";
  }

  public boolean chgHDI(int cid, int year, float newHDI){
   return false;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
   return false;        
  }
  
  public String listCountryLanguages(int cid){
    return "";
  }
  
  public boolean updateHeight(int cid, int decrH){
    return false;
  }
    
  public boolean updateDB(){
    return false;    
  }
  
  
  /* MAIN METHOD, FOR TESTING PURPOSES ONLY!!!!!!
   * MUST DELETE BEFORE FINAL SUBMISSION!!!!!!!!!!
   */
  public static void main(String[] args){
      Assignment2 assy = new Assignment2();
      assy.connectDB("jdbc:postgresql://localhost:5432/csc343h-g3luke","g3luke","");
      
      assy.insertCountry(84,"Wonderland",96,32);
      
      assy.disconnectDB();
  }
}
