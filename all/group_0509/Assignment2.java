import java.sql.*;
import java.io.*;

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
	catch (ClassNotFoundException e){
	}
 
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
 
  public boolean connectDB(String URL, String username, String password){
      try{ 
	connection = DriverManager.getConnection(URL, username, password);
	sql = connection.createStatement();
	sql.executeUpdate("Set search_path To A2"); 
	return true;
	}catch (SQLException e){
		return false;
	}
      
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
	try{
		connection.close();
		return true;
	}catch (SQLException e){
		return false;
	}
        
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
	try{
		String sqlText = "INSERT INTO country " +
			 			  "VALUES (?, ?, ?, ?);";
		ps= connection.prepareStatement(sqlText);
		ps.setInt(1, cid);
		ps.setString(2, name);
		ps.setInt(3, height);
		ps.setInt(4, population);
		ps.executeUpdate();
		ps.close();
		return true;
	}catch(SQLException e){
	   return false;
	}
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	try{
		String sqlText = "SELECT count(cid) as c FROM oceanaccess where oid = ? group by oid;";
		ps = connection.prepareStatement(sqlText);
		ps.setInt(1, oid);
		rs = ps.executeQuery();
		int toRet = -1;
		while (rs.next()){
			toRet = rs.getInt("c");
		}
		ps.close();
		rs.close();
		return toRet;

	}catch(SQLException e){
		return -1;
	}  
  }
   
  public String getOceanInfo(int oid){
	try{

		String sqlText = "SELECT * FROM ocean WHERE oid = ?;";
		ps=connection.prepareStatement(sqlText);
		ps.setInt(1, oid);
		rs = ps.executeQuery();	
		String toRet = "";
		while(rs.next()){
			toRet = rs.getInt("oid") + ":" + rs.getString("oname")+":"+rs.getInt("depth"); 
		}		
		ps.close();
		rs.close();
		return toRet;
		
	}catch(SQLException e){
   		return "";
	}
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	try{
		String sqlText = "UPDATE hdi SET hdi_score = ? where cid = ? and year = ?;";
		ps=connection.prepareStatement(sqlText);
		ps.setFloat(1, newHDI);
		ps.setInt(2, cid);
		ps.setInt(3, year);
		ps.executeUpdate();
		ps.close();
		return true;

	}catch(SQLException e){


   		return false;
	}
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	try{
		String sqlText = "DELETE FROM neighbour WHERE country = ? and neighbor = ?;";
		ps = connection.prepareStatement(sqlText);
		ps.setInt(1, c1id);
		ps.setInt(2, c2id);
		ps.executeUpdate();
		ps.setInt(1, c2id);
		ps.setInt(2, c1id);
		ps.executeUpdate();
		ps.close();	
		return true;
	}catch(SQLException e){
		return false;
	}        
  }
  
  public String listCountryLanguages(int cid){
	try{
		
		String sqlText = "SELECT language.lid as l, language.lname as n, SUM(language.lpercentage * country.population) as pop FROM country Join Language on language.cid = country.cid WHERE country.cid = ? GROUP BY language.lname, language.lid ORDER BY pop;";

		String toRet = "";
		ps = connection.prepareStatement(sqlText);
		ps.setInt(1, cid);

		rs = ps.executeQuery();
		
		while(rs.next()){
			toRet += rs.getInt("l") + ":" + rs.getString("n") + ":" + rs.getFloat("pop")+"#";
		}

		return toRet;
		
	}catch(SQLException e){

		return "";
	}
	
  }
  
  public boolean updateHeight(int cid, int decrH){
	try{
		String sqlText = "SELECT height FROM country where cid = ?;";
		
		ps = connection.prepareStatement(sqlText);
		ps.setInt(1, cid);
		rs=ps.executeQuery();
		int height = 0;
		while (rs.next()){
		  height = rs.getInt("height");
		}
		int newHeight = height - decrH;
		sqlText = "Update country SET height = ? where cid = ?;";
		ps = connection.prepareStatement(sqlText);
		ps.setInt(1, newHeight);
		ps.setInt(2, cid);
		ps.executeUpdate();
		rs.close();
		ps.close();
		return true;
	}catch(SQLException e){
    		return false;
	}
  }
    
  public boolean updateDB(){
	try{
		String sqlText = "DROP TABLE IF EXISTS mostPopulousCountries CASCADE;"
			       + "CREATE TABLE mostPopulousCountries( cid INTEGER, cname VARCHAR(20));";
		
		ps = connection.prepareStatement(sqlText);
		ps.executeUpdate();

		sqlText = "INSERT INTO mostPopulousCountries(SELECT cid, cname FROM country WHERE population > 100000000);";
		ps = connection.prepareStatement(sqlText);
		ps.executeUpdate();
		ps.close();
		return true;
	}catch(SQLException e){
		return false;    
	}

  }

  
}
