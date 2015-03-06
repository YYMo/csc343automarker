import java.sql.*;
import java.util.List;
import java.util.ArrayList;

public class Assignment2 {
    
  // A connection to the database  
  Connection connection;
  
  // Statement to run queries, "static(not changing)" and does not accept input at runtime
  Statement sql;
  
  // Prepared Statement, allows input at runtime
  PreparedStatement ps;
  
  // Resultset for the query
  ResultSet rs;
  
  //CONSTRUCTOR
  Assignment2(){
	try {
		Class.forName("org.postgresql.Driver");
	}
	catch (ClassNotFoundException e) {
		System.out.println("Failed to find the JDBC driver");
	}
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	try {
		connection = DriverManager.getConnection(URL, username, password);
	}
	catch (SQLException e) {
		System.out.println("Failed to find the JDBC driver");
		return false;
	}
	return true;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
	try {
		if (sql != null)	
			sql.close();
		if (ps != null)
			ps.close();
		if (rs != null)
			rs.close();
		connection.close();
	} catch (SQLException se) {
		System.out.println("Failed to close JDBC connection");
		return false;
	}
	
	return true;    
  }
  
  private void setSearchPath () {
	try {
		sql = connection.createStatement();
		sql.executeUpdate("SET search_path TO a2");
	} catch (SQLException se) {
		System.out.println("Could not set search path");
	}
  }

  public boolean insertCountry (int cid, String name, int height, int population) {
	List <Integer> cids = new ArrayList<>();
	
	setSearchPath();
	
	try {
		sql = connection.createStatement();
		rs = sql.executeQuery("SELECT cid FROM country");
		
		while (rs.next())
			cids.add(rs.getInt("cid"));
		
		
		if (cids.contains(cid))
			return false;
			
		ps = connection.prepareStatement("INSERT INTO country " +
										 "(cid, cname, height, population) VALUES " +
										 "(?, ?, ?, ?)");
		ps.setInt(1, cid);
		ps.setString(2, name);
		ps.setInt(3, height);
		ps.setInt(4, population);
		ps.executeUpdate();
	} catch (SQLException se) {
		System.out.println(se.getMessage());
	} finally {
		try { if (sql != null) sql.close(); } catch (Exception e) {};
		try { if (ps != null) ps.close(); } catch (Exception e) {};
		try { if (rs != null) rs.close(); } catch (Exception e) {};
	}
	
	return true;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	int count = 0;
	
	setSearchPath();
	
	try {
		ps = connection.prepareStatement("SELECT DISTINCT cid FROM oceanAccess WHERE oid = ?");
		ps.setInt(1, oid);
		rs = ps.executeQuery();
		while (rs.next()) {
			count++;
		}
	} catch (SQLException se) {
		System.out.println(se.getMessage());
		return -1;
	} finally {
		try { if (ps != null) ps.close(); } catch (Exception e) {};
		try { if (rs != null) rs.close(); } catch (Exception e) {};
	}
	return count;  
  }
   
  public String getOceanInfo(int oid){
	String result = "";
	
	setSearchPath();
	
	try {
		ps = connection.prepareStatement("SELECT oid, oname, depth FROM ocean WHERE oid = ?");
		ps.setInt(1, oid);
		rs = ps.executeQuery();

		while (rs.next()) {
			result += (rs.getInt("oid") + ":" + rs.getString("oname") + ":" + rs.getInt("depth"));
		}
	} catch (SQLException se) {
		System.out.println(se.getMessage());
		return "";
	} finally {
		try { if (ps != null) ps.close(); } catch (Exception e) {};
		try { if (rs != null) rs.close(); } catch (Exception e) {};
	}
	return result;
  }
        
  public boolean chgHDI(int cid, int year, float newHDI){
	setSearchPath();
	
	try {
		ps = connection.prepareStatement("SELECT * FROM hdi WHERE cid = ? AND year = ?");
		ps.setInt(1, cid);
		ps.setInt(2, year);
		rs = ps.executeQuery();
		
		if (!rs.next())
			return false;
			
		ps = connection.prepareStatement("UPDATE hdi SET hdi_score = ? WHERE cid = ? AND year = ?");
		ps.setFloat(1, newHDI);
		ps.setInt(2, cid);
		ps.setInt(3, year);
		ps.executeUpdate();
	} catch (SQLException se) {
		System.out.println(se.getMessage());
		return false;
	} finally {
		try { if (ps != null) ps.close(); } catch (Exception e) {};
		try { if (rs != null) rs.close(); } catch (Exception e) {};
	}
	return true;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	setSearchPath();
	
	try {
		ps = connection.prepareStatement("DELETE FROM neighbour WHERE country = ? AND neighbor = ? OR " + 
										 "country = ? AND neighbor = ?");
		ps.setInt(1, c1id);
		ps.setInt(2, c2id);
		ps.setInt(3, c2id);
		ps.setInt(4, c1id);
		ps.executeUpdate();
	} catch (SQLException se) {
		System.out.println(se.getMessage());
		return false;
	} finally {
		try { if (ps != null) ps.close(); } catch (Exception e) {};
	}
	return true;
  }
  
  public String listCountryLanguages(int cid){
	String result = "";
	
	setSearchPath();
	
	try {
		ps = connection.prepareStatement("SELECT l.lid AS lid, lname, lpercentage, population " + 
										 "FROM language l JOIN country c ON l.cid = c.cid " +
										 "WHERE c.cid = ? " + 
										 "ORDER BY (lpercentage * population) ASC");
		ps.setInt(1, cid);
		rs = ps.executeQuery();
		
		if (rs.next())
			result += (rs.getInt("lid") + ":" + rs.getString("lname") + ":" +
						(rs.getFloat("lpercentage") * rs.getInt("population")));
	
		while (rs.next()) {
			result += ("#" + rs.getInt("lid") + ":" + rs.getString("lname") + ":" +
						(rs.getFloat("lpercentage") * rs.getInt("population")));
		}
	} catch (SQLException se) {
		System.out.println(se.getMessage());
		return "";
	} finally {
		try { if (ps != null) ps.close(); } catch (Exception e) {};
		try { if (rs != null) rs.close(); } catch (Exception e) {};
	}
	return result;
  }
  
  public boolean updateHeight(int cid, int decrH){
	setSearchPath(); 
	  
	try {
		ps = connection.prepareStatement("SELECT * FROM country WHERE cid = ?");
		ps.setInt(1, cid);
		rs = ps.executeQuery();
		
		if (!rs.next())
			return false;
		
		ps = connection.prepareStatement("UPDATE country SET height = height - ? WHERE cid = ?");
		ps.setInt(1, decrH);
		ps.setInt(2, cid);
		ps.executeUpdate();
	} catch (SQLException se) {
		System.out.println(se.getMessage());
		return false;
	} finally {
		try { if (ps != null) ps.close(); } catch (Exception e) {};
	}
	return true;
  }

  public boolean updateDB(){
	setSearchPath();  
	  
	try {
		ps = connection.prepareStatement("CREATE TABLE mostPopulousCountries2 (" +
											"cid 	INTEGER," +
											"cname 	VARCHAR(20))");
		ps.executeUpdate();
		ps = connection.prepareStatement("INSERT INTO mostPopulousCountries2 " +
										 "SELECT cid, cname FROM country WHERE population >= 100000000");
		ps.executeUpdate();
	} catch (SQLException se) {
		System.out.println(se.getMessage());
		return false;
	} finally {
		try { if (ps != null) ps.close(); } catch (Exception e) {};
	}
	return true;
  }
}

