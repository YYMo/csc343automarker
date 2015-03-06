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
      return true;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      return true;    
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
   return true;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	return count(oid)
	else
		return -1;  
  }
   
  public String getOceanInfo(int oid){
   return "oid:oname:depth";
	else
		return "";
  }

  public boolean chgHDI(int cid, int year, float newHDI){
   return true;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
   return true;        
  }
  
  public String listCountryLanguages(int cid){
	return "";
  }
  
  public boolean updateHeight(int cid, int decrH){
    return true;
  }
    
  public boolean updateDB(){
	return true;    
  }
  
}
