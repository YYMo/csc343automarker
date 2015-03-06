
import java.sql.*;

public class Assignment2 {
  
  //Path to jdbc
  String url;
  
  //String to read queries into
  String query;
  
  // A connection to the database  
  Connection connection;
  
  // Statement to run queries
  Statement sql;
  
  // Prepared Statement
  PreparedStatement ps;
  
  // Resultset for the query
  ResultSet rs;
  
  //Constructor
  Assignment2(){
  try {
		Class.forName("org.postgresql.Driver");
	    }
		//THIS CAN MAYBE BE TAKEN OUT
	    catch (ClassNotFoundException e) {
			return;
		}
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){

	  try {
                connection = DriverManager.getConnection(URL, username, password);
	  	}
		catch (SQLException se) {
            return false;
        }
	
	if (connection != null){
		return true;
	} else {
		return false;
	}  
	
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
	try {
		connection.close();
	} catch (SQLException e) {
		return false;
	}
	
	if (connection != null) {
		return false;
	}
    return true;    
  }
  
  public boolean insertCountry (int cid, String name, int height, int population) {
	
	String exists = "0";
	
	query = "SELECT count(*) AS number" +
			"FROM country" +
			"WHERE cid=" + Integer.toString(cid);
	

	try {
		rs = sql.executeQuery(query);
	
	while(rs.next()){
		exists = rs.getString("number");
	}
	rs.close();
	
	
	if (exists == "0"){
		int affected;
	
		query = "INSERT INTO country" +
		    	"VALUES (" + Integer.toString(cid) + ", '" + name + "', " + Integer.toString(height) + ", " + Integer.toString(population) + ")";
		
		affected = sql.executeUpdate(query);
		
		if(affected > 0){
			return true;
		}else{
			return false;
		}
	}else{
		return false;
	}
	} catch (SQLException e) {
		return false;
	}
	
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	
	String oceans = null;
	int oceannum;
	
	query = "SELECT count(cid) AS number" +
		  "FROM oceanAccess" +
		  "WHERE oid=" + Integer.toString(oid);
	
	
	try {
		rs = sql.executeQuery(query);
	
	
	if(rs.next()){
		while(rs.next()){
			oceans = rs.getString("number");
		}
	
		rs.close();
		
		oceannum = Integer.parseInt(oceans);
		return oceannum;
	} else{	
		rs.close();
		return -1;  
	}
	} catch (SQLException e) {
		return -1;
	}
  }
   
  public String getOceanInfo(int oid){
    
	String oname;
	String depth;
	String result = null;
	
	query = "SELECT oid, oname, depth" +
		  "FROM ocean" +
		  "WHERE oid =" + oid;
		  
	try {
		rs = sql.executeQuery(query);
	
	if(rs.next()){
		while(rs.next()){
			oid = rs.getString("oid");
			oname = rs.getString("oname");
			depth = rs.getString("depth");
			result = oid + ":" + oname + ":" + depth;
		}
		rs.close();
	}
	if(result != null){
		return result;
	}else{
		rs.close();
		return "";
	}
	} catch (SQLException e) {
		return "";
	}
  }

  public boolean chgHDI(int cid, int year, float newHDI){
    
	int affected = 0;
	
	query = "UPDATE hdi" +
		  "SET hdi_score=" + Float.toString(newHDI) +
		  "WHERE cid=" + Integer.toString(cid) + " AND  year=" + Integer.toString(year);
		  
	try {
		affected = sql.executeUpdate(query);
	} catch (SQLException e) {
		return false;
	}
	
	if(affected > 0){
		return true;
	}else{
		return false;
	}
  }

  public boolean deleteNeighbour(int c1id, int c2id){
    
	int affected = 0;
	
	query = "DELETE from neighbour" + 
		  "WHERE (country=" + Integer.toString(c1id) + " AND neighbor=" + Integer.toString(c2id) + ") OR (country=" + 
		  Integer.toString(c2id) + " AND neighbor=" + Integer.toString(c1id) + ")";
		  
	try {
		affected = sql.executeUpdate(query);
	} catch (SQLException e) {
		return false;
	}
	
	if(affected > 0){
		return true;
	}else{
		return false;
	}        
  }
  
  public String listCountryLanguages(int cid){
	
	String lid;
	String lname;
	String lspeakers;
	String result = "";		
		
	query = "SELECT lid, lname, (lpercentage * population) AS lspeakers" +
		  "FROM language, country" +
		  "WHERE cid=" + Integer.toString(cid) +
		  "ORDER BY lspeakers DESC";
		  
	try {
		rs = sql.executeQuery(query);
	
	
	if(rs.next()){
		while(rs.next()){
			lid = rs.getString("lid");
			lname = rs.getString("lname");
			lspeakers = rs.getString("lspeakers");
			result = result + lid + ":" + lname + ":" + lspeakers + "#";
		}
		rs.close();
		return result;
	}else{
		rs.close();
		return "";
	}
	} catch (SQLException e) {
		return "";
	}
  }
  
  public boolean updateHeight(int cid, int decrH){
	
	int affected = 0;
	
	query = "UPDATE country" + 
		  "SET height=height-" + Integer.toString(decrH) +
		  "WHERE cid=" + Integer.toString(cid);
		  
	try {
		affected = sql.executeUpdate(query);
	} catch (SQLException e) {
		return false;
	}
	
    if(affected > 0){
		return true;
	}else{
		return false;
	}
  }
    
  public boolean updateDB(){
	
	boolean affected = false;
	
	query = "CREATE TABLE mostPopulousCountries(" + 
		  "cid INTEGER" +
		  "cname VARCHAR(20)" + 
		  "PRIMARY KEY(cid)" +
		  "FOREIGN KEY(cid)" +
		  "REFERENCES country(cid)" +
		  "FOREIGN KEY(cname)" +
		  "REFERENCES country(cname)" +
		  "ORDER BY cid ASC";
		  
	try {
		affected = sql.execute(query);
	} catch (SQLException e) {
		return false;
	}
	
	if(affected){
		return true;
	}else{
		return false;
	}
  }
  
}