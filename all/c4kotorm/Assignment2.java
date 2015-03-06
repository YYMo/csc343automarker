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
		catch (ClassNotFoundException e) {
			//System.out.println("Failed to find the JDBC driver");
		}
	}
  
  	//Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password){
		try{
        	connection = DriverManager.getConnection("jdbc:postgresql://"+URL, username,password);
        	ps = connection.prepareStatement("Set search_path to a2;");
			ps.executeQuery();
			ps.close();
		}catch(Exception ex){
			//System.out.println(ex);
			return false;
		}
		return true;
	}
  
	//Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB(){
	  	boolean hasClosed = true;
		try{
			if (connection != null)
				 connection.close();
		}catch(Exception ex){
			hasClosed = false;
		}

		try{
			if (rs != null)
				 rs.close();
		}catch(Exception ex){
			hasClosed = false;
		}

		try{
			if (sql != null)
				 sql.close();
		}catch(Exception ex){
			hasClosed = false;
		}

		return hasClosed;
	}
    
	public boolean insertCountry (int cid, String name, int height, int population) {
		try{
		String query = "INSERT INTO country (cid, cname, height, population ) VALUES (?, ?, ?, ?)";
		ps = connection.prepareStatement(query);
		ps.setInt(1,cid);
		ps.setString(2,name);
		ps.setInt(3,height);
		ps.setInt(4, population);
		ps.executeUpdate();
		ps.close();
		//	ps.commit();
		ps.close();
		return true;
		}catch(Exception e)
		{
			//System.out.println("insertCountry" + e);
			return false;
		}
	}
  
	public int getCountriesNextToOceanCount(int oid) {
		try
		{
			String query = "SELECT COUNT(*) as c FROM oceanaccess WHERE oid = ?";
			ps = connection.prepareStatement(query);
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			//while(rs.next()){
         		//Retrieve by column name
			int count;
			if(rs.next())
         		count  = rs.getInt("c");
         	else
         		count = -1;
      		//}
      		rs.close();
      		ps.close();
			return count;
		}catch(Exception e)
		{
			//System.out.println("getCountriesNextToOceanCount"+e);
			return -1;
		}
	}
   
	public String getOceanInfo(int oid){
		try {
			String query = "SELECT oid, oname, depth FROM ocean WHERE oid = ?";
			ps = connection.prepareStatement(query);
			ps.setInt(1,oid);
			rs = ps.executeQuery();
			String result = "";
			if(rs.next())
			{
				result += rs.getInt("oid") + ":" + 
								rs.getString("oname") + ":" + 
								rs.getInt("depth");
			}
			rs.close();
			ps.close();
			return result;
		} catch(Exception ex) {
			//System.out.println("getOceanInfo" + ex);
			return "";
		}
	}

	public boolean chgHDI(int cid, int year, float newHDI){
			//Try 
			try {
				String query = "UPDATE hdi SET hdi_score = ? WHERE cid= ? AND year= ?";
				ps = connection.prepareStatement(query);
				ps.setFloat(1, newHDI);
				ps.setInt(2, cid);
				ps.setInt(3, year);
				ps.executeUpdate();
				ps.close();
				return true;
			}catch(Exception ex)
			{
				//System.out.println("chgHDI" + ex);
				return false;
			}
	}

	public boolean deleteNeighbour(int c1id, int c2id){
		try
		{
			String query = "DELETE FROM neighbour WHERE country = ? AND neighbor = ? OR neighbor= ? AND country = ?";
			ps = connection.prepareStatement(query);
			ps.setFloat(1, c1id);
			ps.setInt(2, c2id);
			ps.setInt(3, c1id);
			ps.setInt(4, c2id);
			ps.executeUpdate();
			ps.close();
			return true;       
		}catch(Exception e)
		{
			//System.out.println("deleteNeighbour" + e);
			return false;
		}
	}

	public String listCountryLanguages(int cid){
		try{
			String query = "SELECT lid, lname, (country.population * language.lpercentage)  AS population "
							+ "FROM language "
							+ "JOIN country "
							+ "ON language.cid = country.cid "
							+ "WHERE country.cid = ?";

			//Now go though every row and append in the format
			ps = connection.prepareStatement(query);
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			String resultSet = "";
			while(rs.next()){
	     		//Retrieve by column name
	     		resultSet += rs.getInt("lid") + ":" + rs.getString("lname") + ":" + rs.getInt("population") + "#";
	  		}
	  		rs.close();
	  		ps.close();
	  		return resultSet;
	  	}catch(Exception e)
	  	{
	  		//System.out.println("listCountryLanguages" + e);

	  		return "";
	  	}	
	}

	public boolean updateHeight(int cid, int decrH){
		try{
			String query = "UPDATE country SET height = ? WHERE cid = ?";
			ps = connection.prepareStatement(query);
			ps.setInt(1, decrH);
			ps.setInt(2, cid);
			ps.executeUpdate();
			ps.close();
			return true;
		}catch(Exception e){
			//System.out.println("updateHeight" + e);
			return false;
		}
	}
    
	public boolean updateDB(){
		try
		{
			String query = "DROP TABLE IF EXISTS mostPopulousCountries CASCADE; CREATE TABLE mostPopulousCountries ("
							+ "cid 		INTEGER 	PRIMARY KEY,"
	    					+ "cname 		VARCHAR(20));";
			ps = connection.prepareStatement(query);
			ps.executeUpdate();
			ps.close();

			query = "INSERT INTO mostPopulousCountries (cid, cname)  SELECT cid, cname FROM country WHERE population >= 100000000";
			ps = connection.prepareStatement(query);
			ps.executeUpdate();
			ps.close();
		}catch(Exception ex)
		{
			//System.out.println("updateDB" + ex);

			return false;
		}
		return true;
	}
}
