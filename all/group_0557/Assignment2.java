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
    
    // Query
    String query;
  
    //CONSTRUCTOR
    Assignment2(){
	try {
	    Class.forName("org.postgresql.Driver");
	}
	catch (ClassNotFoundException e) {
	    //System.out.println("Failed to find the JDBC driver");
	    return;
	}
    }
  
    //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password){


        try {
            connection = DriverManager.getConnection(URL, username, password);
	    return true;
	    
	}
	catch (SQLException e) {
	    return false;
	}	
    }
  
    //Closes the connection. Returns true if closure was sucessful
    public boolean disconnectDB(){
        try {
		    connection.close();
		    return true;
		}
		catch (Exception e) {
		    return false;
		}
    }
     
    public boolean insertCountry (int cid, String name, int height, int population) {
        query = "INSERT INTO a2.country " + 
				"VALUES (?, ?, ?, ?) ";
    	try {
	        ps = connection.prepareStatement(query);
	        ps.setInt(1, cid);
	        ps.setString(2, name);
	        ps.setInt(3, height);
	        ps.setInt(4, population);
	        ps.executeUpdate();
	        return ps.getUpdateCount() > 0;
    	}
    	catch (Exception e) {
    		return false;
    	}
    }
  
    public int getCountriesNextToOceanCount(int oid) {
    	query = "select count(*) " +
    			"from a2.oceanAccess " + 
    			"where oid = ?";
    	int count = 0;
    	try {
    		ps = connection.prepareStatement(query);
    		ps.setInt(1, oid);
    		rs = ps.executeQuery();
    	 	if(rs.next()) {
				count = rs.getInt(1);
			}
    		if (count == 0) {
    			return -1;
    		} 
    		return count;
    	}
    	catch (Exception e) {
    		return -1;
    	}
    }
   
    public String getOceanInfo(int oid){
    	query = "select * " +
    			"from a2.ocean " +
    			"where oid = ?";
    	String info = "";
    	try {
    		ps = connection.prepareStatement(query);
    		ps.setInt(1, oid);
    		rs = ps.executeQuery();
    		while (rs.next()) {
    			info = rs.getInt("oid") + ":" + rs.getString("oname") 
    											+ ":" + rs.getInt("depth");
    		}
    		return info;
    	}
    	catch (Exception e) {
    		return "";
    	}
    }

    public boolean chgHDI(int cid, int year, float newHDI){
        query = "UPDATE a2.hdi " +
        		"SET hdi_score = ? " + 
        		"where cid = ? and year = ?"; 
        try {
    		ps = connection.prepareStatement(query);
    		ps.setFloat(1, newHDI); 
    		ps.setInt(2, cid);
    		ps.setInt(3, year);
    		ps.executeUpdate();
    		return (ps.getUpdateCount() > 0);
    	}
    	catch (Exception e) {
    		return false;
    	}
    }

    public boolean deleteNeighbour(int c1id, int c2id){
        query = "DELETE from a2.neighbour " + 
				"where (country = ? and neighbor = ?) " + 
				"or (country = ? and neighbor = ?)"; 
		try {
			ps = connection.prepareStatement(query);
			ps.setInt(1, c1id); 
			ps.setInt(2, c2id);
			ps.setInt(3, c2id);
			ps.setInt(4, c1id);
			ps.executeUpdate();
			return (ps.getUpdateCount() > 0);
		}
		catch (Exception e) {
			return false;
		}      
    }
  
    public String listCountryLanguages(int cid){
    	query = "select lid, cname, (lpercentage * population) as lpopulation " +
    			"from  a2.country natural join a2.language " + 
    			"where country.cid = ?" + 
    			"order by lpopulation";
    	String languageList = "";
    	try {
    		ps = connection.prepareStatement(query);
    		ps.setInt(1, cid);
    		rs = ps.executeQuery();
    		//String cname = rs.getString("cname");
    		while (rs.next()) {
    			languageList += rs.getInt("lid") + ":" + rs.getString("cname") + ":" 
    								+ rs.getFloat("lpopulation") + "#";
    		}
    		if (languageList != "") {
    			return languageList.substring(0, languageList.length()-1);
    		}
    		return "";
    	}
    	catch (Exception e) {
    		return "";
    	}
    }
  
    public boolean updateHeight(int cid, int decrH){
        query = "UPDATE a2.country " + 
        		"SET height = height - ? " + 
        		"where cid = ?"; 
		try {
			ps = connection.prepareStatement(query);
			ps.setInt(1, decrH); 
			ps.setInt(2, cid);
			ps.executeUpdate();
			return (ps.getUpdateCount() > 0);
		}
		catch (Exception e) {
			return false;
		}
    }
    
    public boolean updateDB(){
        try {
        	sql = connection.createStatement();
        	
        	query = "SET search_path to A2";
        	sql.executeUpdate(query);
        	
        	query = "DROP TABLE IF EXISTS mostPopulousCountries CASCADE";
        	sql.executeUpdate(query);
        	
        	query = "CREATE TABLE mostPopulousCountries ( " +
        				"cid int, cname varchar(20))";
        	sql.executeUpdate(query);
        	
        	query = "INSERT INTO mostPopulousCountries (" +
        				"select cid, cname " +
        				"from a2.country " +
        				"where population > 100000000 " +
        				"order by cid)";
        	sql.executeUpdate(query);
        	return true;
        }
        catch (Exception e) {
        	return false;
        }
    }
}
