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
  Assignment2()
  {
	  
	  try {
			
			// Load JDBC driver
			Class.forName("org.postgresql.Driver");

		} catch (ClassNotFoundException e) {

			//System.out.println("Where is your PostgreSQL JDBC Driver? Include in your library path!");
			//e.printStackTrace();
			return;

		} 
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      
	  try {
			
			//Make the connection to the database, ****** but replace "username" with your username ******
			//connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/csc343h-c4vasant", "c4vasant", "");
			connection = DriverManager.getConnection(URL, username, password);
			
			return true;
		} catch (SQLException e) {

			return false;

		}  
	  
	  
  }
  
  //Closes the connection. Returns true if closure was successful
  public boolean disconnectDB()
  {
      try {
    	  connection.close();
    	  return true;
      }
	  catch(Exception e){
		  
		  return false;
	  }
  }
  //-------------------------------------------------------------------------------
  public boolean insertCountry (int cid, String name, int height, int population)
  {
	  try{
		   
			//Create a Statement for executing SQL queries
			sql = connection.createStatement(); 

			//---------------------------------------------------------------------------------------
			//use prepared statement for insertion
			String sqlText;
			sqlText = "INSERT INTO a2.country " +
				  	"VALUES(?,?,?,?)	    ";
			ps = connection.prepareStatement(sqlText);
			ps.setInt(1,cid);
			ps.setString(2,name);
			ps.setInt(3,height);
			ps.setInt(4,population);
			ps.executeUpdate();
			ps.close();
  			return true;
	  }
	  catch (SQLException e) {

          return false;

	  }	
	  
  }
//-------------------------------------------------------------------------------
//--DONE
  public int getCountriesNextToOceanCount(int oid) {
	
	  try {
		  sql = connection.createStatement(); 
	  
	      String sqlText;
		  sqlText = "SELECT count(oid) FROM a2.oceanAccess " +
			  	  "GROUP BY oid HAVING  oid=?    ";
		  ps = connection.prepareStatement(sqlText);
		  ps.setInt(1,oid);
		  ResultSet rs=ps.executeQuery();
		  rs.next();
		  int r=rs.getInt(1);
		  rs.close();
		  ps.close();
		  return r;
	  }
	  catch (SQLException e) {
		  return -1;
	  }
  }
  //-------------------------------------------------------------------------------
//--DONE
  public String getOceanInfo(int oid){
	  try {
		  sql = connection.createStatement(); 
		  String sqlText;
		  sqlText = "SELECT oid,oname,depth FROM a2.ocean WHERE ocean.oid=? " ;
		  ps = connection.prepareStatement(sqlText);
		  ps.setInt(1,oid);
		  ResultSet rs=ps.executeQuery();
		  String output="";
		  while (rs.next()) 
		  {
			  output+=rs.getString("oid")+":"+rs.getString("oname")+":"+rs.getString("depth");
		  }        
		  rs.close();
		  ps.close();
		  return output;
	  } catch (SQLException e){
		  return "";
	  }
  }
//-------------------------------------------------------------------------------
//done
  public boolean chgHDI(int cid, int year, float newHDI){
	  try {
		  sql = connection.createStatement(); 
		  
		  String sqlText;
		  sqlText = "UPDATE a2.hdi SET hdi_score = ? WHERE cid=? AND year= ? ";
		  ps = connection.prepareStatement(sqlText);
		  ps.setFloat(1,newHDI);
		  ps.setInt(2,cid);

		  ps.setInt(3,year);

		  ps.executeUpdate();
		  ps.close();
		  return true;
	  } catch (SQLException e){
		  return false;
	  }
   
  }
//-------------------------------------------------------------------------------
 //done
  public boolean deleteNeighbour(int c1id, int c2id)
  {
	  try {
		  sql = connection.createStatement(); 
		  
		  String sqlText,sqlText2;
		  sqlText = "DELETE FROM a2.neighbour WHERE country=? AND neighbor=? " ;
		  sqlText2= "DELETE FROM a2.neighbour WHERE country=? AND neighbor=? ";
		  ps = connection.prepareStatement(sqlText);
		  ps.setInt(1,c1id);
		  ps.setInt(2,c2id);
		  ps.executeUpdate();
		  ps.close();
		  
		  ps = connection.prepareStatement(sqlText2);
		  ps.setInt(1,c2id);
		  ps.setInt(2,c1id);
		  ps.executeUpdate();
		  ps.close();
		  return true;
	  } catch (SQLException e){
	
		  return false;
	  }      
  }
//-------------------------------------------------------------------------------
//done
  public String listCountryLanguages(int cid){
	  try {
		  sql = connection.createStatement(); 
		  String sqlText;
		  sqlText = "SELECT lid,lname,(lpercentage*population/100) AS lpop FROM a2.language l JOIN a2.country c "+
		            "ON l.cid=c.cid " +
		  		    "WHERE l.cid=? ORDER BY (lpop)" ;
		  ps = connection.prepareStatement(sqlText);
		  ps.setInt(1,cid);
	
		  ResultSet rs=ps.executeQuery();

		  String output="";
		  while (rs.next()) 
		  {
			  output+=rs.getString("lid")+":"+rs.getString("lname")+":"+rs.getString("lpop")+"\n";
			  
		  }        
		  rs.close();
		  ps.close();
		  return output;
	  } catch (SQLException e){
		  return "";
	  }
  }
//-------------------------------------------------------------------------------
//done
  public boolean updateHeight(int cid, int decrH){
	  try {
		  sql = connection.createStatement(); 
		  
		  String sqlText;
		  sqlText = "UPDATE a2.country SET height=(height-(?)) WHERE cid=? " ;
		  ps = connection.prepareStatement(sqlText);
		  ps.setInt(1, decrH);
		  ps.setInt(2,cid);
		  ps.executeUpdate();
		  ps.close();
		  return true;
	  } catch (SQLException e){

		  return false;
	  }  
    
  }
//-------------------------------------------------------------------------------

  public boolean updateDB(){
	  try {
		  sql = connection.createStatement(); 
		  
		  String sqlText;
		  sqlText = "DROP TABLE IF EXISTS a2.mostPopulousCountries; " +
		  		"CREATE TABLE a2.mostPopulousCountries(cid INTEGER, cname VARCHAR(20));";
		  ps = connection.prepareStatement(sqlText);
		  ps.executeUpdate();
		  ps.close();
	
		  
		  sqlText="CREATE VIEW temp AS " +
		  		"(SELECT cid,cname FROM a2.country WHERE population>100000000 ORDER BY cid) ; "+
		  		  //"INSERT INTO a2.mostPopulousCountries (SELECT * FROM temp);"+
		  		  "DROP VIEW IF EXISTS temp;";
		  
		  
		  sqlText="SELECT cid,cname FROM a2.country" + 
	  			  "WHERE population>100000000 ORDER BY cid) ; ";
	  		 
		  
		  ps=connection.prepareStatement(sqlText);
		  ResultSet rs=ps.executeQuery();
		  ps.close();
		 while(rs.next())
		  {
			  sqlText="INSERT INTO (a2.mostPopulousCountries (cid,cname)) + VALUES(?,?)";
			  ps=connection.prepareStatement(sqlText);
			  ps.setInt(1,rs.getInt("cid"));
			  ps.setString(2,rs.getString("cname"));
		  }
		 ps.executeUpdate();
		 ps.close();
		 rs.close();
		  return true;
	  } catch (SQLException e){
		  System.out.println("Query Execution failed!"); 
		  return false;
	  }    
  }
  
  
}
