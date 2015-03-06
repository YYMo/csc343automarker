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
	  Class.forName("org.postgresql.Driver")
  }
  
  //Using the input parameters, establish a connection to be used for this 
  //session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	  try {
		  connection = DriverManager.getConnection(url, username, password)
	  } catch (SQLException e){
		  
	  }
	  if (connection != null){
		  return true;
	  }else{
		  return false;
	  }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try {
    	  connection.close()
      } catch (SQLException e){
    	  
      }
      if (connection != null){
    	  return false
      }else{
    	  return true
      }
      
  }
  
  //Inserts a row into the country table. cid is the country id of the country, 
  //name is the name of the country, height is the highest elevation of the 
  //country and population is the population of the country. You have to check 
  //if the cid already exists. Returns true if insertion is successful.
  public boolean insertCountry (int cid, String name, int height, int 
		  population) {
	  try{
		  PreparedStatement ps = connection.prepareStatement("SELECT * FROM " +
	  		"country WHERE cid=?");
		  ps.setInt(1,cid);
		  ResultSet rs = ps.executeQuery();
	  }
	  
	  if (rs != null){
		  rs.close()
		  Statement st = connection.createStatement();
		  String sqlquery = "INSERT INTO country(cid, cname, height, " +
		  		"population) VALUES(" + cid + "," + name + "," + height + "," 
		  		+ population + ")";
		  ResultSet rs = st.executeQuery(sqlquery);
		  rs.close();
		  st.close();
		  ps.close();
		  return(true)
	  }else{
		  rs.close()
		  ps.close()
		  return false
	  }
  }
  
  //Returns the number of countries in the table "oceanAccess" that are located
  //next to the ocean with id oid. Returns -1 if an error occurs.
  public int int getCountriesNextToOceanCount(int oid) {
	return -1;  
  }
  
  //Returns a string with the information of an ocean with id oid. The output is
  //"oid:oname:depth". Returns an empty string "" if the ocean does not exist.
  public String getOceanInfo(int oid){
   return "";
  }

  //Changes the HDI value of the country cid for the year year to the HDI value
  //supplied (newHDI). Returns true if the change was successful, false
  //otherwise.
  public boolean chgHDI(int cid, int year, float newHDI){
   return false;
  }

  //Deletes the neighboring relation between two countries. Returns true if the
  //deletion was successful, false otherwise. You can assume that the 
  //neighboring relation to be deleted exists in the database. Remember that if 
  //c2 is a neighbor of c1, c1 is also a neighbour of c2.
  public boolean deleteNeighbour(int c1id, c2id){
   return false;        
  }
  
  //Returns a string with all the languages that are spoken in the country with 
  //id cid. The list of languages should follow the contiguous format described 
  //above, and contain the following attributes in the order shown: (NOTE: 
  //before creating the string order your results by population).
  //l1id:l1lname:l1population#l2id:l2lname:l2population#... 
  //where:
  //- lid is the id of the language.
  //- lname is name of the country.
  //- population is the number of people in a country that speak the language, 
  //note that you will need to compute this number, as it is not readily 
  //available in the database.
  //Returns an empty string if the country does not exist.
  public String listCountryLanguages(int cid){
	return "";
  }
  
  //Decreases the height of the country with id cid. (A decrease might happen 
  //due to natural erosion.) Returns true if the update was successful, false 
  //otherwise.
  public boolean updateHeight(int cid, int decrH){
    return false;
  }
  
  //Create a table containing all the countries which have a population over 100
  //million. The name of the table should be mostPopulousCountries and the 
  //attributes should be:
  //- cid INTEGER (country id)
  //- cname VARCHAR(20) (country name)
  //Returns true if the database was successfully updated, false otherwise. 
  //Store the results in ASC order according to the country id (cid).
  public boolean updateDB(){
	return false;    
  }
  
}
