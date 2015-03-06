import java.sql.*;
import java.util.*;
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
	public boolean connectDB(String URL, String username, String password) {
		try {
			Class.forName("org.postgresql.Driver");
		}
		catch (ClassNotFoundException e) {
			return false;
		}
		try {
			connection = DriverManager.getConnection(URL, username, password);
			return true;
	  	} catch (Exception e) {
			//System.out.println(e.toString());
			return false;
		}
	}
 
	//Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB(){
		//rs.close;
		//sql.close();
		try{
			if(connection!=null){
				connection.close();
				return true;
			} else
				return false;
		}catch(SQLException se){
			se.printStackTrace();
			return false;
		}
	}

	public boolean insertCountry (int cid, String name, int height, int population) {
    
		String queryString = "INSERT INTO a2.country(cid, cname, height, population) VALUES(?, ?, ?, ?)";
		// cid, name, height, population
		try {
		  //rs = ps.executeQuery();

			ps = connection.prepareStatement(queryString);
			ps.setInt(1, cid);
			ps.setString(2,name);
			ps.setInt(3, height);
			ps.setInt(4, population);
			ps.executeUpdate();
			ps.close();
			return true;
		  
		} catch (SQLException ex) {
		  System.err.println("SQL Exception." + "<Message>:" + ex.getMessage());

		  return false;
		}
	}
  
	public int getCountriesNextToOceanCount(int oid) {
		String queryString = "select count(oid) from a2.oceanAccess where oid ='" + oid + "' group by cid";
		try {
			ps = connection.prepareStatement(queryString);
			rs = ps.executeQuery();

			if(!rs.next()){
				ps.close();
				rs.close();
				return -1;  
			}
			else{
				int val =  ((Number) rs.getObject(1)).intValue();
				ps.close();
				rs.close();
				return val;
			}
		} catch(SQLException e) {
			System.out.println(e);
			return -1;
		}
	}
   
	/*
	Returns a string with the information of an ocean with id oid. The output is 
	oid:oname:depth. Returns an empty string if the ocean does not exist.
	*/   
	public String getOceanInfo(int oid){
		String queryString = "select * from a2.ocean where oid ='" + oid;
		try {
			ps = connection.prepareStatement(queryString);
			rs = ps.executeQuery();
			String result;

			if(!rs.next()){
				result = "";
			}
			else{
				int roid = rs.getInt("oid");
				String oname = rs.getString("oname");
				int depth = rs.getInt("depth");
	
				result = roid + ":" + oname + ":" + depth;
			}
			ps.close();
			rs.close();
			return result;
		} catch (SQLException e) {
			return "";
		}
	}


	public boolean chgHDI(int cid, int year, float newHDI){
		StringBuilder stm = new StringBuilder("UPDATE a2.hdi ");
		stm.append("SET hdi_score = ? WHERE cid = ? and year = ?");
    
		try {
			ps = connection.prepareStatement(stm.toString());
			ps.setFloat(1, newHDI);
			ps.setInt(2, cid);
			ps.setInt(3, year);
			int roleUpdated = ps.executeUpdate();
			if (roleUpdated == 1) {
				ps.close();
				rs.close();
				return true;
			}
			else {
				//ps.close();
				//rs.close();
				return false;
			}
		} catch (Exception e) {
			System.out.println(e);
			return false;
		}
	}

	public boolean deleteNeighbour(int c1id, int c2id){

		String queryString1 = "DELETE FROM a2.neighbour WHERE a2.country = " + c1id + " and neighbor = " + c2id;
		String queryString2 = "DELETE FROM a2.neighbour WHERE a2.country = " + c2id + " and neighbor = " + c1id;
		try {
			ps = connection.prepareStatement(queryString1);
			rs = ps.executeQuery();
			if(!rs.next()){
				ps.close();
				rs.close();
				return false;
			}
			ps = connection.prepareStatement(queryString2);
			rs = ps.executeQuery();
			if(!rs.next()){
				ps.close();
				rs.close();
				return false;
			}
			ps.close();
			rs.close();
			return true;
		} catch(SQLException e) {
			System.out.println(e);
			return false;
		}
	}
  
	public String listCountryLanguages(int cid){
		try {
		  
			String result = "";
			String queryString = " SELECT  lid, lname, (lpercentage * population) as population from a2.language as lan join a2.country as con on lan.cid = con.cid where con.cid= " + String.valueOf(cid);

			ps = connection.prepareStatement(queryString);
			rs = ps.executeQuery();
			while (rs.next()){
				int lid = rs.getInt("lid");
				String lname = rs.getString("lname");
				float population = rs.getFloat("population");
				result += String.valueOf(lid) + ":" + lname + ":" +  String.valueOf(population) + "#";
			}
			ps.close();
			rs.close();
			return result.substring(0,result.length()-1);
		}
		catch (SQLException ex) {    
			return "";
		}
	}
  
	public boolean updateHeight(int cid, int decrH){
		String queryString = "UPDATE a2.country SET height=" + decrH + "WHERE cid =" + cid;
		try {
			ps = connection.prepareStatement(queryString);
			rs = ps.executeQuery();
			if(!rs.next()){
				//ps.close();
				//rs.close();
				return false;
			}
			else{
				ps.close();
				rs.close();
				return true;
			}
		} catch(SQLException e) {
			return false;
		}
	}
    
	public boolean updateDB(){
		String mostPopularCountries = "SELECT cid, cname FROM a2.country WHERE population > 100000000 ORDER BY cid ASC";
		String createTableStm = "CREATE TABLE a2.mostPopularCountries (cid INTEGER, cname VARCHAR(20))";
		String populateStm = "INSERT INTO a2.mostPopularCountries VALUES (?, ?)";
		
		//String drop = "DROP TABLE IF EXIST a2.mostPopularCountries";	
		//String countStm = "SELECT count(*) FROM a2.country WHERE population > 100000000";
		try {
			//sql.executeQuery(drop);
			//ps = connection.prepareStatement(countStm);
			//rs = ps.executeQuery();
			//int count = rs.getInt();
			ps = connection.prepareStatement(mostPopularCountries);
			rs = ps.executeQuery();
			//ps.close();
			ps = connection.prepareStatement(createTableStm);
			ps.executeQuery();
			//ps.close();
			ps = connection.prepareStatement(populateStm);
			//if(count > 0) {
				while (rs.next()) {
					int cid = rs.getInt(1);
					String cname = rs.getString(2);
					ps.setInt(1, cid);
					ps.setString(2, cname);
					ps.executeUpdate();
				}
			//} else
			//	return true;
			//rs.close();
			//ps.close();
			return true;
		} catch (Exception e) {
			System.out.println(e);
			return false;
		}
	}
}
