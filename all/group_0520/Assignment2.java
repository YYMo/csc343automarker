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
	Assignment2(){	
		try {
			Class.forName("org.postgresql.Driver");
		
		} catch (ClassNotFoundException e) {
			return;
		}	
	}
  
  	//Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password){  
		try {
			connection = DriverManager.getConnection(URL, username, password);
			return true;
		
		} catch (SQLException e) {
			return false;
		}  
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
	  
		try {
			ps = connection.prepareStatement("INSERT INTO a2.country(cid, cname, height, population) " +
											"VALUES(?, ?, ?, ?)");
			
			ps.setInt(1, cid);
			ps.setString(2, name);
			ps.setInt(3, height);
			ps.setInt(4, population);
			ps.executeUpdate();
		
			ps.close(); 
			return true;
		
		} catch (SQLException e) {
			return false;
		}		
	}
  
	public int getCountriesNextToOceanCount(int oid) {
		
		try {
			int output = 0;

			ps = connection.prepareStatement("SELECT COUNT(cid) FROM a2.oceanAccess WHERE oid=?");
	  
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			
			while (rs.next()) {
				output = rs.getInt(1);
			}
			
			ps.close();
			rs.close();
			return output;
	  
		} catch (SQLException e) {
			return -1;
	  	}
	}
   
	public String getOceanInfo(int oid){
		
		try {
			String output = "";
	  
			ps = connection.prepareStatement("SELECT * FROM a2.ocean WHERE oid=?");
		
			ps.setInt(1, oid);
			rs = ps.executeQuery();
		
			while (rs.next()) {
				output = rs.getString(1) + ":" + rs.getString(2) + ":" + rs.getString(3);
			}
			
			ps.close();
			rs.close();
			return output;
			
		} catch (SQLException e) {
			return "";
		}
   
	}

	public boolean chgHDI(int cid, int year, float newHDI){
		
		try {
			ps = connection.prepareStatement("UPDATE a2.hdi SET hdi_score=? WHERE cid=? AND year=?");
			
			ps.setFloat(1, newHDI);
			ps.setInt(2, cid);
			ps.setInt(3, year);
			ps.executeUpdate();
			
			ps.close();
			return true;
		
		} catch (SQLException e) {
			return false;
		}
	}

  	public boolean deleteNeighbour(int c1id, int c2id){
  		
  		try {
			ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE " +
											"(country=? AND neighbor=?) OR (country=? AND neighbor=?)");
			
			ps.setInt(1, c1id);
			ps.setInt(2, c2id);
			ps.setInt(3, c2id);
			ps.setInt(4, c1id);
			ps.executeUpdate();
			
			ps.close();
			return true;
		
		} catch (SQLException e) {
			return false;
		}        
  	}
  
  	public String listCountryLanguages(int cid){
  		
  		try {
  			String output = "";
  			float population = 0;

  			ps = connection.prepareStatement("SELECT lid, lname, lpercentage, population FROM " +
  											"(a2.language JOIN a2.country ON a2.language.cid=a2.country.cid) " +
  											"WHERE a2.language.cid=?");

  			ps.setInt(1, cid);
  			rs = ps.executeQuery();

  			while (rs.next()) {
  				population =  (rs.getFloat(3)/100) * rs.getInt(4);
  				output = output.concat(rs.getString(1) + ":" + rs.getString(2) + ":" + Float.toString(population) + "#");
  			}

  			output = output.substring(0, output.length()-1);
  			ps.close();
  			rs.close();
  			return output;

  		} catch (SQLException e) {
  			return "";
  		}
  	}
  
	public boolean updateHeight(int cid, int decrH){
		
		try {
			ps = connection.prepareStatement("SELECT height FROM a2.country WHERE cid=?");
			
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			
			int oldHeight = 0;
		
			while (rs.next()) {
				oldHeight = rs.getInt(1);
			}
			
			ps.close();
			rs.close();
			
			ps = connection.prepareStatement("UPDATE a2.country SET height=? WHERE cid=?");
			
			ps.setInt(1, oldHeight - decrH);
			ps.setInt(2, cid);
			ps.executeUpdate();
			
			ps.close();
			return true;
		
		} catch (SQLException e) {
			return false;
		}
	}
    
	public boolean updateDB(){
		
		try {
			ps = connection.prepareStatement("CREATE TABLE a2.mostPopulousCountries AS " +
											"(SELECT cid, cname FROM a2.country WHERE population>? ORDER BY cid ASC)");
			
			ps.setInt(1, 100000000);
			ps.executeUpdate();
			
			ps.close();
			return true;
			
		} catch (SQLException e) {
			return false;    
		}
	}
  
}
