import java.sql.*;

public class Assignment2 {
    
  // A connection to the database  
  Connection connection;
  
  // Statement to run queries
  Statement sql;
  
  // Prepared Statement
  PreparedStatement ps;
  
  // Result set for the query
  ResultSet rs;

  //CONSTRUCTOR
  Assignment2(){
	  try{
		//load the driver for PostgreSQL
		  Class.forName("org.postgresql.Driver");
		/*  connectDB( URL, username, password);  
		  sql = connection.createStatement();
		  rs = sql.executeQuery("select name, number from pcmtable where number < 2");
		  while( rs.next() )
			  System.out.println(rs.getString(1) + " (" + rs.getInt(2) + ")");
		  rs.close();
		  sql.close();
		  connection.close();*/
	  }catch(Exception e){//System.err.println(e); 
	  }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	//connect to the db
	  try{	connection = DriverManager.getConnection(URL, username, password);
	  sql = connection.createStatement();
      return true;
	  }catch(Exception e){//System.err.println(e); 
		  return false;
	  }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
	  try{rs.close(); 
		  sql.close();	
		  connection.close();
      return true;
	  }catch(Exception e){//System.err.println(e); 
		  return false;
	  }  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
	  try{// CHECK IF THE CID EXISTS ALREADY
      String stm = "INSERT INTO a2.country"
      		+ "VALUES ("
      		+ cid
      		+ ","
      		+ name
      		+ ","
      		+ height
      		+ ","
      		+ population
      		+ ")";
      sql.executeUpdate(stm);
      return true;
	  }catch(Exception e){
		  return false;
	  }
   
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	  
	  try{
		  sql = connection.createStatement();
		  rs = sql.executeQuery("select oid, count(cid) "
		  		+ "from a2.oceanAccess "
		  		+ "where oid ="
		  		+ oid
		  		+ "group by oid");
		  return rs.getInt(2);
	  }catch(Exception e){
		  return -1;
	  }
	  
  }
   
  public String getOceanInfo(int oid){
	  try{
	  rs = sql.executeQuery("select oid, oname, depth"
	  		+ "from a2.ocean"
	  		+ "where oid ="
	  		+ oid);
	  String stm = (rs.getInt(1) + ":" + rs.getString(2) + ":"+rs.getInt(3));
   return stm;}catch(Exception e){
	   return "";
   }
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	  try{// CHECK IF THE CID EXISTS ALREADY
	      String stm = "UPDATE a2.hdi"
	      		+ "SET hdi_score ="
	      		+ newHDI
	      		+" where cid ="
	      		+ cid
	      		+ "AND year ="
	      		+ year;
	      sql.executeUpdate(stm);
	      return true;
		  }catch(Exception e){
			  return false;
		  }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	  try{
	  String stm1 = "DELETE FROM a2.neighbour"
	  		+ "where country ="
	  		+ c1id
	  		+ "AND neighbor ="
	  		+ c2id;
	  String stm2 = "DELETE FROM a2.neighbour"
		  		+ "where country ="
		  		+ c2id
		  		+ "AND neighbor ="
		  		+ c1id;
	  sql.executeUpdate(stm1);
	  sql.executeUpdate(stm2);
	  return true;
  }catch(Exception e){
	  return false;
  }    
  }
  
  public String listCountryLanguages(int cid){
	  try{
		  String result="";
		  String stm1 = "SELECT *"
		  		+ "FROM a2.language"
		  		+ "WHERE cid ="
		  		+ cid;
		  String stm1b = "SELECT cid, population AS pop FROM a2.country";
		  String stm2 = "SELECT lid, lname, pop*lpercentage AS population"
		  		+ "FROM "
				+ stm1b
				+  " JOIN ("
		  		+ stm1
		  		+ ") ON cid "
		  		+ "ORDER BY population";
		  
		  rs = sql.executeQuery(stm2);
		  while(rs.next()){
			  result += (rs.getInt(1) + ":" + rs.getString(2) + ":"+rs.getInt(3))+"#";
		  }
		   result.substring(0,result.length()-1);
		  return result;
	  }catch(Exception e){
		  return "";
	  }
  }
  
  public boolean updateHeight(int cid, int decrH){
	  try{// CHECK IF THE CID EXISTS ALREADY
		  
		  String stm1 = "SELECT height"
			  		+ "FROM a2.country"
			  		+ "WHERE cid ="
			  		+ cid;
		  rs = sql.executeQuery(stm1);
		 int value = rs.getInt(1);
	      String stm2 = "UPDATE a2.country"
	      		+ "SET height ="
	      		+ (value-decrH)
	      		+" where cid ="
	      		+ cid;
	      sql.executeUpdate(stm2);
	      return true;
		  }catch(Exception e){
			  return false;
		  }
  }
    
  public boolean updateDB(){
try{// CHECK IF THE CID EXISTS ALREADY
		  
		  String stm1 = "CREATE TABLE mostPopulousCountries"
				  +"cid INTIGER REFRENCES country(cid)"
				  + "cname VARCHAR(20)  REFRENCES country (cname)"
				  + "PRIMARY KEY (cid)";
			  		
		 sql.executeUpdate(stm1);
		 String stm2 = "SELECT cid, cname"
		 		+ "FROM a2.country"
		 		+ "WHERE population > 100e6"
		 		+ "ORDER BY cid ASC";
		 rs = sql.executeQuery(stm2);
		 
		 while(rs.next()){
			  sql.executeUpdate("INSERT INTO mostPopulousCountries"
			      		+ "VALUES ("
			      		+ rs.getInt(1)
			      		+ ","
			      		+ rs.getString(2)
			      		+ ")");
		  }

	      return true;
		  }catch(Exception e){
			  return false;
		  }
  
}
  }

