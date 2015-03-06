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
      }
    catch (ClassNotFoundException e) {
      System.out.println("Failed to find the JDBC driver");
        }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password) throws SQLException{
    connection = DriverManager.getConnection(URL, username, password);
    return true;
  }
  
  //Closes the connection. Returns true if closure was successful
  public boolean disconnectDB(){
    try {
      connection.close();
      return true;
    }
    catch (Exception e){
      return false;
    }  
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
    
   return false;
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
  
  public static void main(String[] args){
    System.out.println("Hello World!");
  }
}