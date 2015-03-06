
import java.sql.*;
import java.util.*;

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
	  try{
		  Class.forName("org.postgresql.Driver");
	  }catch(ClassNotFoundException e){
		  System.out.println("Someone forgot to include postgres library.");
	  }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	  Properties props = new Properties();
	  props.setProperty("user", username);
	  props.setProperty("password", password);
	  props.setProperty("SSL", "true");
	  
	  String IRL = "jdbc:postgresql://"+URL+";";
	  try{		 
		  connection = DriverManager.getConnection(IRL,props);
	  }catch(SQLException sql){
		  System.out.println("Connection Failed! Check your URL, username and password : "+sql.toString());
	  }
	  if(connection!=null){
		  return true;
	  }
      return false;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
	try {
		connection.close();
		return true;
	} catch (SQLException e) {
		return false;
	}
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
	  String query = "INSERT INTO a2.country(cid,cname,height,population) VALUES (?,?,?,?);";
	  try {
		ps = connection.prepareStatement(query);
		ps.setInt(1, cid);
		ps.setString(2,name);
		ps.setInt(3,height);
		ps.setInt(4, population);
		if(ps.executeUpdate()>0){
			return true;
		}
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} 
	  return false;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	  String query = "SELECT COUNT(DISTINCT cid) AS num FROM a2.oceanAccess WHERE oid=?;";
	  try {
		ps = connection.prepareStatement(query);
		ps.setInt(1, oid);
		rs=ps.executeQuery();
		rs.next();
		return rs.getInt("num");
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} 
	  return -1;  
  }
   
  public String getOceanInfo(int oid){
	  String query = "SELECT * from a2.ocean WHERE oid = ?;";
	  try{
		  ps = connection.prepareStatement(query);
		  ps.setInt(1,oid);
		  rs=ps.executeQuery();
		  rs.next();
		  return String.format("%d:%s:%d",rs.getInt(1),rs.getString(2),rs.getInt(3));
	  }catch(SQLException e){
		// TODO Auto-generated catch block
			e.printStackTrace();
	  }
	  return "";
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	  String query = "UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? AND year = ?;";
	  try {
		ps = connection.prepareStatement(query);
		ps.setFloat(1,newHDI);
		ps.setInt(2, cid);
		ps.setInt(3,year);
		if(ps.executeUpdate()>0){
				return true;
		}
	  } catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	  } 
	  return false;
  }

  public boolean deleteNeighbour(int c1id,int c2id){
	  String query = "DELETE FROM a2.neighbour WHERE (country = ? AND neighbor = ? );";
	  try {
		ps = connection.prepareStatement(query);
		ps.setInt(1,c1id);
		ps.setInt(2, c2id);		
		if(ps.execute()){
			ps.setInt(2,c1id);
			ps.setInt(1,c2id);
			if(ps.executeUpdate()>0){
				return true;
			}
		}
	  } catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	  } 
	  return false;
  }
  
  public String listCountryLanguages(int cid){
	  String query = "SELECT lid,lname,(lpercentage*population) AS pop "
	  		+ "FROM a2.language,a2.country WHERE country.cid=language.cid AND country.cid = ?"
			 +"GROUP BY lid,lname,population,lpercentage ORDER BY population DESC;" ;
	  try{
		  ps = connection.prepareStatement(query);
		  ps.setInt(1,cid);
		  rs=ps.executeQuery();
		  String result = "";
	  while(rs.next()){
			  result = result.concat(String.format("%s:%s:%s#", rs.getString(1),rs.getString(2),rs.getString(3)));
		  }
		  return result.substring(0, result.length()-1); // remove the last # char 
	  }catch(SQLException e){
		// TODO Auto-generated catch block
			e.printStackTrace();
	  }
	  return "";
  }
  
  public boolean updateHeight(int cid, int decrH){
	  String query = "UPDATE a2.country SET height = height - ? WHERE cid = ?;";
	  try {
		ps = connection.prepareStatement(query);
		ps.setInt(1,decrH);
		ps.setInt(2,cid);
		System.out.println(ps.toString());
		if(ps.executeUpdate()>0){
			return true;
		}
	  } catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	  } 
	  return false;
  }
    
  public boolean updateDB(){
		boolean success = false;
	  String query = "SELECT cid,cname from a2.country "
			  +"WHERE population >= 100000000 "
			  +"ORDER BY cid ASC";
	  try{
		  ps = connection.prepareStatement(query);
		  rs=ps.executeQuery();
		  query = "DROP TABLE IF EXISTS a2.mostPopulousCountries CASCADE;"+
				"CREATE TABLE a2.mostPopulousCountries ("
				  +"cid INTEGER PRIMARY KEY,"
				  +"cname VARCHAR(20) NOT NULL);";
		  ps = connection.prepareStatement(query);
		  if(ps.executeUpdate()>0){
			  while(rs.next()){
				  query = "INSERT INTO a2.mostPopulousCountries(cid,cname)"
						  +"VALUES(?,?)";
				  ps = connection.prepareStatement(query);
				  ps.setInt(1, rs.getInt(1));
				  ps.setString(2, rs.getString(2));
				  ps.executeUpdate();
				  success = true;
				}						
			}
		  }
	  }catch(SQLException e){
		// TODO Auto-generated catch block
			e.printStackTrace();
	  }
	return success;    
  }
}
