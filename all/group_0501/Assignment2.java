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
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
    try {
  	 Class.forName("org.postgresql.Driver");
  	}
      catch (ClassNotFoundException e) {
  	return false;
  	}
  	try{
    	connection = DriverManager.getConnection(URL, username, password);
  	 return true;
  	}
  	catch (SQLException e){
  	 return false;	
  	}
      
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
    try{ 
    	connection.close(); 
    	return true;
    } 
	catch(Exception ex){ 
		return false;
	}
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
   String query = "insert into a2.country values (" + String.valueOf(cid) + ", " + "'" + name + "'" + ", " + String.valueOf(height) + ", " + String.valueOf(population) + ")";
   String checkQuery = "select count(cid) as count from a2.country where cid = " + String.valueOf(cid);
   int count = 0;
   try{
	   ps = connection.prepareStatement(checkQuery);
	   rs = ps.executeQuery();

	   while (rs.next()) {
		count = rs.getInt("count");
	   }
	}
	catch(SQLException se){
		return false;	
	}

    if (count > 0){
	return false;	
    }

    try{	
	   ps = connection.prepareStatement(query);
	   ps.executeUpdate();
	}
	catch(SQLException se){
		return false;	
	}

	try{ 
    	rs.close();
	} 
	catch (Exception e1) 
	{ 
		return false;
	}
    try{ 
    	ps.close(); 
    } 
	catch (Exception e2) 
	{
		return false;
	}
	return true;
}
  
  public int getCountriesNextToOceanCount(int oid) {
	String subquery =  "select cid from a2.oceanAccess where oid = " + String.valueOf(oid);
	String query = "select count(cid) as count from (" + subquery + ") oa";
	int count = 0;
	try{
		ps = connection.prepareStatement(query);
		rs = ps.executeQuery();
		while (rs.next()){
			count = rs.getInt("count");
		}
	}
	catch(SQLException se){
		return -1;
	}

	try{ 
    	rs.close();
	} 
	catch (Exception e1) 
	{ 
		return -1;
	}
    try{ 
    	ps.close(); 
    } 
	catch (Exception e2) 
	{
		return -1;
	}
	return count;
	
  }
   
  public String getOceanInfo(int oid){
	 String oname = "";
	 int depth = 0;
	 String ans;
	 String query = "select oid, oname, depth from a2.ocean where oid = " + String.valueOf(oid);

	 try{
	   ps = connection.prepareStatement(query);
	   rs = ps.executeQuery();

      while (rs.next()) {

        oname = rs.getString("oname");
        depth = rs.getInt("depth");
      }
	
	}
	  catch(SQLException se){
		  return "";	
	}
	ans = String.valueOf(oid) + ":" + oname + ":" + String.valueOf(depth);
	
	try{ 
    	rs.close();
	} 
	catch (Exception e1) 
	{ 
		return "";
	}
    try{ 
    	ps.close(); 
    } 
	catch (Exception e2) 
	{
		return "";
	}
	return ans;
}

  public boolean chgHDI(int cid, int year, float newHDI){
	String query = "update a2.hdi set hdi_score=" + newHDI + " where cid = " + String.valueOf(cid) + " and year = " + String.valueOf(year);
	try{	
		ps = connection.prepareStatement(query);
		ps.executeUpdate();
	}
	catch(SQLException se){
		return false;
	}

    try{ 
    	ps.close(); 
    } 
	catch (Exception e2) 
	{
		return false;
	}

	return true;
}

  public boolean deleteNeighbour(int c1id, int c2id){
	String query1 = "DELETE FROM a2.neighbour WHERE country=" + String.valueOf(c1id) + " AND neighbor=" + String.valueOf(c2id);
	String query2 = "DELETE FROM a2.neighbour WHERE country=" + String.valueOf(c2id) + " AND neighbor=" + String.valueOf(c1id);
	try{
		ps = connection.prepareStatement(query1);
		ps.executeUpdate();
	}
	catch(SQLException	se){
		return false;
	}
	try{
		ps = connection.prepareStatement(query2);
		ps.executeUpdate();
	}
	catch(SQLException	se){
		return false;
	}

    try{ 
    	ps.close(); 
    } 
	catch (Exception e2) 
	{
		return false;
	}
	
   return true;        
  }
  
  public String listCountryLanguages(int cid){
  	String ans = "";
  	int lid = 0;
  	String lname = "";
  	int population = 0;
  	String subquery = "select cid, lid, lname, lpercentage from a2.language where cid = " + String.valueOf(cid);
  	String query2 = "select LP.lid, LP.lname, (LP.lpercentage*a2.country.population) as population from (" + subquery + ") LP JOIN a2.country ON a2.country.cid = LP.cid ORDER BY population";

	try{
		ps = connection.prepareStatement(query2);
		rs = ps.executeQuery();

		while(rs.next()){
			lid = rs.getInt("lid");
			lname = rs.getString("lname");
			population = rs.getInt("population");
			ans += String.valueOf(lid) + ":" + lname + ":" + String.valueOf(population) + "#";
		}
	}
		catch(SQLException se){
		  return "";	
	}
	try{ 
    	rs.close();
	} 
	catch (Exception e1) 
	{ 
		return "";
	}
    try{ 
    	ps.close(); 
    } 
	catch (Exception e2) 
	{
		return "";
	}
	String formated = ans.substring(0, ans.length()-1);
	return ans;
  }
  
  public boolean updateHeight(int cid, int decrH){
    String query = "update a2.country set height= (height-" + String.valueOf(decrH) + ") where cid = " + String.valueOf(cid);
	try{	
		ps = connection.prepareStatement(query);
		ps.executeUpdate();
	}
	catch(SQLException se){
		return false;
	}
    try{ 
    	ps.close(); 
    } 
	catch (Exception e2) 
	{
		return false;
	}

	return true;
}
    

  public boolean updateDB(){
  	String query1 = "CREATE TABLE a2.mostPopulousCountries " +
                   "(cid INTEGER not NULL, " +
                   " cname VARCHAR(20), " + 
                   " PRIMARY KEY (cid))"; 
	String query2 = "insert into a2.mostPopulousCountries values (select cid, cname from a2.country where population > 100000000)";
	try{
		ps = connection.prepareStatement(query1);
		ps.executeUpdate();
	}
	catch(SQLException se){
		return false;
	}
	try{
		ps = connection.prepareStatement(query2);
		ps.executeUpdate();
	}
	catch(SQLException se){
		return false;
	}

    try{ 
    	ps.close(); 
    } 
	catch (Exception e2) 
	{
		return false;
	}

	return true;    
  }
  
}
