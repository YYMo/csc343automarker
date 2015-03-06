// Student Names: Julian Chow, Shu Xu
// Date: Nov 7 2014
//
//
//

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
  public  boolean connectDB(String URL, String username, String password) throws SQLException {
     try {
	 connection =  DriverManager.getConnection(URL, username, password);
         }
	catch (SQLException ex) {
	 return false;  
	 }
       return true;	 
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB() throws SQLException{
      try {
	 connection.close();	
	} 
	catch (SQLException ex) {
         return false;
	}
	 return true;
  }

 public boolean insertCountry (int cid, String name, int height, int population) throws SQLException {

    try{
      // Prepared Statement
     PreparedStatement  ps = connection.prepareStatement(
        "INSERT INTO a2.country(cid, cname, height, population) VALUES(?, ?, ?, ?)"
        );

      ps.setInt(1, cid);
      ps.setString(2,name);
      ps.setInt(3, height);
      ps.setInt(4, population);
      ps.executeUpdate();
      ps.close();
    }
    catch (SQLException ex) {
	System.err.println("SQL Exception." + "<Message>:" + ex.getMessage());
      return false;
   
    }
      return true;
  }
    
  public int getCountriesNextToOceanCount(int oid) throws SQLException {
	try {
	// prepare Statement
		ps = connection.prepareStatement( "select count(cid) from a2.oceanaccess where oid = ?" 	);
		ps.setInt(1, oid);
		rs = ps.executeQuery();
		rs.next();
		int re = rs.getInt("count");
		rs.close();
		ps.close();
		return (re);
	}
	catch(SQLException ex){
		return -1;
	}  
  }
   
  public String getOceanInfo(int oid) throws SQLException {
   	try {
	// prepare statement 
		ps = connection.prepareStatement("select * from a2.ocean where oid = ?") ;
		ps.setInt(1, oid);
		rs = ps.executeQuery();
		rs.next();
		String oname = rs.getString("oname");
		int depth = rs.getInt("depth");
		rs.close();
		ps.close();
		return (String.valueOf(oid)+ ":"+ oname + ":" + String.valueOf(depth) );
	}
	catch (SQLException ex) {
		return "";
	}

  }

  public boolean chgHDI(int cid, int year, float newHDI) throws SQLException {
        try {
	// prepare statement
		ps = connection.prepareStatement("UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? AND year =?");
		ps.setFloat(1,newHDI);
		ps.setInt(2, cid);
		ps.setInt(3, year);
		ps.executeUpdate();
		ps.close();
		
	}
	catch (SQLException ex) {
	 return false;
	}
	 return true;
  }

  public boolean deleteNeighbour(int c1id, int c2id) throws SQLException {
  	try {
	// prepare statement 
		ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE country = ? AND neighbor = ?");
		ps.setInt(1,c1id);
		ps.setInt(2,c2id);
		ps.executeUpdate();
		ps.close();
		ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE country = ? AND neighbor = ?");
  		ps.setInt(1,c2id); 
                ps.setInt(2,c1id);
		ps.close();
	}
	catch (SQLException ex) {
	 return false;        
	}
	return true;
  }
  
  public String listCountryLanguages(int cid) throws SQLException{
	try {
	// prepare statement 
		ps = connection.prepareStatement(" SELECT  lid, lname, (lpercentage * population) as population from a2.language as l join a2.country as c on l.cid = c.cid where c.cid=?");
		ps.setInt(1,cid);
		rs = ps.executeQuery();
		String out = "";
		while (rs.next()){
			int lid = rs.getInt("lid");
			String lname = rs.getString("lname");
			float population = rs.getFloat("population");
			out += String.valueOf(lid) + ":" + lname + ":" +  String.valueOf(population) + "#";
		}
		ps.close();
		rs.close();
		return out.substring(0,out.length()-1);
	}
	catch (SQLException ex) {	
		return "";
	}
  }
  
  public boolean updateHeight(int cid, int decrH) throws SQLException{
    try {
	// prepare statement 
		ps = connection.prepareStatement("SELECT height FROM a2.country WHERE cid = ?");
                ps.setInt(1, cid);
                rs = ps.executeQuery();
                int height = rs.getInt("height");
                ps.close();
		rs.close();
		ps = connection.prepareStatement("UPDATE a2.country SET height = ? WHERE cid = ?");
		ps.setInt(1, (height - decrH));
		ps.setInt(2, cid);
		ps.executeUpdate();
		ps.close();
	}
	catch (SQLException ex){
	    return false;
	}
	return true;
  }
    
  public boolean updateDB(){
	try {
	 //prepare statement
		ps = connection.prepareStatement("CREATE TABLE a2.mostPopulousCountries AS SELECT cid, cname FROM a2.country where population >= 100000000 order by cid");
		ps.executeUpdate();
		ps.close();
	}
	catch (SQLException ex) {
	return false;    
	}
	return true;
  }

    
}


