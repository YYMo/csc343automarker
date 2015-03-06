import java.sql.*;
import java.sql.DriverManager;

public class Assignment2 {
    
  // A connection to the database  
  Connection connection;
  
  // Statement to run queries
  String sql;
  
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
    }
  }
  
  
  

  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password) throws SQLException {
    try {
    	connection = DriverManager.getConnection(URL, username, password);
	  } catch (Exception e) {
		  return false;
	  }

      return true;
	  
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
	  try {
		  connection.close();		  
	  } catch (Exception e) {
		  return false;
	  }
      return true;    
  }
//  Inserts a row into the country table. cid is the name of the country, name
//  is the name of the country, height is the highest elevation point and population
//  is the population of the newly inserted country. You have to check if the country 
//  with id cid exists. Returns true if the insertion was successful, false otherwise. 
  public boolean insertCountry (int cid, String name, int height, int population) {
  
	  sql = "SELECT count(*) FROM a2.country WHERE cid="+Integer.toString(cid) +";";
	  try{
		  rs = query(sql);
		  
		  rs.next();
		  int count = rs.getInt("count");
		  // if cid exists, exit. 
		  if (count > 0) {	
			  return false;
		  }
	
		  else
			  sql = "INSERT INTO a2.country VALUES ("+Integer.toString(cid)+", \'"+name+"\', "+Integer.toString(height)+", "+Integer.toString(population)+" );";
	         boolean success = update(sql);
	         if (success==false) {
	        	 
	        	 return false;
	         }
	      return true;
	  }
      catch(SQLException e){
    	  return false;
      }
  }
//  Returns the number of countries in table "oceanAccess" that are located next to 
//  the ocean with id oid. Returns -1 if an error occurs. 
  public int getCountriesNextToOceanCount(int oid) {
 	  sql = "SELECT count(*) FROM a2.oceanAccess WHERE oceanAccess.oid="+Integer.toString(oid) +";";
	  try{
		  rs = query(sql);
		  rs.next();
		  int count = rs.getInt("count");
		  return count;
	  }
	  catch(SQLException e){
		  return -1;
	  }

	  
  }
//  Returns a string with the information of an ocean with id oid. The output is 
//  "oid:oname:depth". Returns an empty string "" if the ocean does not exist. 
  public String getOceanInfo(int oid){
  	  sql = "SELECT * FROM a2.ocean WHERE oid="+Integer.toString(oid) +";";
	  try {
		  rs = query(sql);	
		  rs.next();
		  String S_oid = Integer.toString(oid);
		  String oname = rs.getString("oname");
		  String depth = Integer.toString(rs.getInt("depth"));
		  
		  return S_oid+":"+oname+":"+depth;
	  }
      catch(SQLException e){
    	  return "";
      }
  }
//  Changes the HDI value of the country cid for the year year to the HDI value 
//  supplied (newHDI). Returns true if the change was successful, false otherwise. 
  public boolean chgHDI(int cid, int year, float newHDI){
	  sql = "UPDATE a2.hdi set hdi_score =" + Float.toString(newHDI)+" where cid=" 
	  			  + Integer.toString(cid) + " AND year=" +Integer.toString(year)+";";
      boolean success = update(sql);
      if (success==false) {
     	 return false;
      }
	  return true;
  }
  
//  Deletes the neighboring relation between two countries. Returns true if the 
//  deletion was successful, false otherwise. You can assume that the neighboring 
//  relation to be deleted exists in the database. Remember that if c2 is a neighbor of 
//  c1, c1 is also a neighbour of c2. 
  public boolean deleteNeighbour(int c1id, int c2id){
	 sql = "DELETE from a2.neighbour where country=" 
     		  + Integer.toString(c1id) + " And neighbor=" + Integer.toString(c2id)+";";
     boolean success = update(sql);
     if (success==false) {
    	 return false;
     }
     
	 sql = "DELETE from a2.neighbour where country=" 
		  	+ Integer.toString(c2id)+ " And neighbor=" + Integer.toString(c1id)+";";
     success = update(sql);
     if (success==false) {
    	 return false;
     }
     return true;        
  }
  
//  Returns a string with all the languages that are spoken in the country with id cid. 
//  The list of languages should follow the contiguous format described above, and 
//  contain the following attributes in the order shown: (NOTE: before creating the 
//  string order your results by population). 
//  
//  "l1id:l1lname:l1population#l2id:l2lname:l2population#... " 
//  where: 
//	  - lid is the id of the language. 
//	  - lname is name of the country. 
//	  - population is the number of people in a country that speak the language, 
//	  note that you will need to compute this number, as it is not readily 
//	  available in the database. 
//
//	  Returns an empty string "" if the country does not exist 
  public String listCountryLanguages(int cid){
	  
	  String result = "";
      sql = "SELECT lid, lname, country.population * language.lpercentage / 100 AS speakers " +
      								"FROM a2.language JOIN a2.country ON language.cid=country.cid where country.cid="
      								+Integer.toString(cid) +" order by speakers DESC;";
      try{
    	  rs = query(sql);

    	 
	      while ( rs.next() ) {
	         int lid = rs.getInt("lid");
	         String  lname = rs.getString("lname");
	         float speakers = rs.getFloat("speakers");
	         result = result + Integer.toString(lid)+":"+lname+":"+Float.toString(speakers)+"#";
	      }
      }
	  catch(SQLException e){
		  return "";
	  }

	  return result;
  }
  

//  Decreases the height of the country with id cid. (A decrease might happen due to 
//  natural erosion.) Returns true if the update was successful, false otherwise. 
  public boolean updateHeight(int cid, int decrH){
	  int height=0;
	  sql = "SELECT height FROM a2.country where cid="+Integer.toString(cid)+";";
      try{
    	  rs = query(sql);
    	  rs.next();
    	  height = rs.getInt("height");
      }
      catch (SQLException e) {
    	  return false;
      }
	  
      sql = "UPDATE a2.country SET height="+Integer.toString((height-decrH))+" where cid="+Integer.toString(cid)+";";
	  boolean success = update(sql);
	  
	  return success;
	  
  }	

//  Create a table containing all the countries which have a population over 100 
//  million. The name of the table should be mostPopulousCountries and the 
//  attributes should be: 
//		- cid INTEGER (country id) 
//  	- cname VARCHAR(20) (country name) 
//  Returns true if the database was successfully updated, false otherwise. Store the 
//  results in ASC order according to the country id (cid). 
  public boolean updateDB(){
      sql = "CREATE TABLE a2.mostPopulousCountries (" +
              "cid 		INTEGER 		PRIMARY KEY, "+
              "cname 	VARCHAR(20)		NOT NULL);";
      if (update(sql)==false) {
    	  return false;
    	  }
      else {
    	  sql = "SELECT cid, cname FROM a2.country WHERE Population>10;";
    	  try{
    	  rs = query(sql);
	      while (rs.next()) {
		         int cid = rs.getInt("cid");
		         String  cname = rs.getString("cname");
		         
		         sql = "INSERT INTO a2.mostPopulousCountries VALUES ("+Integer.toString(cid)+", '"+cname+"');";
		         boolean success = update(sql);
		         if (success==false) {
		        	 return false;
		         }
	      }
	      return true;
    	  }
          catch(SQLException e){
        	  return false;
          }
      }
  }

  
	//  Helper method to execute Queries. Returns the ResultSet object
	public ResultSet query(String query) throws SQLException{
	  try {
		  
	      ps = connection.prepareStatement(query);
	      rs = ps.executeQuery();            
	  }
	  catch (SQLException e) {

		  return rs;
	  }
	  return rs;
	}
	
	//  Helper method to update the Data Base. Returns true if successful, false if not.
	public boolean update(String statement) {
		try {
			
		Statement s = connection.createStatement();
		s.executeUpdate(statement);
	  }
	  catch (SQLException e) {

	      return false ;
	  }
	  return true;
	}

//public static void main( String[] args ) {
//	try{
//		Assignment2 obj1 = new Assignment2();
//		System.out.println("connectDB: " + obj1.connectDB("jdbc:postgresql://localhost:5432/csc343h-c2memmel", "c2memmel", ""));
//		System.out.println("insertCountry: "+ obj1.insertCountry(7,"F", 6, 1000));
//		System.out.println("getCountriesNextToOceanCount: "+ obj1.getCountriesNextToOceanCount(2));
//		System.out.println("getOceanInfo: "+ obj1.getOceanInfo(2));
//		System.out.println("chgHDI: "+ obj1.chgHDI(1, 2010, 6));
//		System.out.println("deleteNeighbour: "+ obj1.deleteNeighbour(3,4));
//		System.out.println("listCountryLanguages: "+ obj1.listCountryLanguages(2));
//		System.out.println("updateHeight: "+ obj1.updateHeight(4, 1));
//		System.out.println("updateDB: "+ obj1.updateDB());
//		System.out.println("disconnectDB: "+ obj1.disconnectDB());
//	}
//	catch (Exception e){
//	}
//}

}
