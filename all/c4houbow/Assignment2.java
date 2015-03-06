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
	}

  
	//Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password){

		try {
			// Load JDBC driver
			Class.forName("org.postgresql.Driver");
	    		connection = DriverManager.getConnection(URL,username,password);
			return connection.isValid(0);
		}catch(SQLException se){
			return false;
		} catch (ClassNotFoundException e) {
 			return false;
		}catch(Exception e){
			return false;
		}
	}
  
	//Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB(){
		try {
			if (sql.isClosed() == false) {
				sql.close();
			}
			if (rs.isClosed() == false) {
				rs.close();
			}
			if (connection.isClosed() == false) {
				connection.close();
			}
			return connection.isClosed();   
		}catch(SQLException se){
			return false;
		}catch(Exception e){
			return false;
		}
	}
    
	public boolean insertCountry (int cid, String name, int height, int population) {
		try {
	    	sql = connection.createStatement();
	    	String selecting = "INSERT INTO a2.country VALUES (" + cid + ",'" + name + "'," + height + "," + population + ");";
	    	sql.executeUpdate(selecting);
	    	sql.close();
			return true;
		}catch(SQLException se){
			return false;
		}catch(Exception e){
			return false;
		}
	}
  
	public int getCountriesNextToOceanCount(int oid) {
		try {
			int numberOfCountry = 0;
			
	    	sql = connection.createStatement();
	    	String selecting = "SELECT count(cid) as numberOfCountry FROM a2.oceanaccess WHERE oid = " + oid + " GROUP BY oid;";
	    	rs = sql.executeQuery(selecting);
	    	while(rs.next()){
	    		numberOfCountry = rs.getInt("numberOfCountry");
	    	}
	    	rs.close();
	    	sql.close();
	    	return numberOfCountry;
		}catch(SQLException se){
			return -1;
		}catch(Exception e){
			return -1;
		}
	}
   
	public String getOceanInfo(int oid){
		try {
			String results = "";
		
	    	sql = connection.createStatement();
	    	String selecting = "SELECT oid, oname, depth FROM a2.ocean WHERE oid = " + oid + ";";
	    	rs = sql.executeQuery(selecting);
	    	while(rs.next()){
	    		int oidN = rs.getInt("oid");
	        	String oname = rs.getString("oname");
	        	int depth = rs.getInt("depth");

	        	results += oidN;
	        	results += ":" + oname;
	        	results += ":" + depth;
	        	if (rs.isLast() == false){
	        		results += '#';
	        	}
	    	}
	    	rs.close();
	    	sql.close();
			return results;
		}catch(SQLException se){
			return "";
		}catch(Exception e){
			return "";
		}
	}

	public boolean chgHDI(int cid, int year, float newHDI){
		try {
	    	sql = connection.createStatement();
	    	String selecting = "UPDATE a2.hdi SET hdi_score = " + newHDI + " WHERE cid = " + cid + " AND year = " + year + ";";
	    	sql.executeUpdate(selecting);
	    	sql.close();
			return true;
		}catch(SQLException se){
			return false;
		}catch(Exception e){
			return false;
		}
	}

	public boolean deleteNeighbour(int c1id, int c2id){
		try {
	    	sql = connection.createStatement();
	    	String selecting = "DELETE FROM a2.neighbour WHERE (country = " + c1id + " AND neighbor = " + c2id + ") OR (country = " + c2id + " AND neighbor = " + c1id + ");";
	    	sql.executeUpdate(selecting);
	    	sql.close();
			return true;
		}catch(SQLException se){
			return false;
		}catch(Exception e){
			return false;
		}    
	}
  
	public String listCountryLanguages(int cid){
		try {
			String results = "";
		
			sql = connection.createStatement();
			String selecting = "SELECT l.lid AS lid, l.lname AS lname, (l.lpercentage * c.population) AS population FROM a2.language l JOIN a2.country c on l.cid = c.cid WHERE c.cid = " + cid + ";";
			rs = sql.executeQuery(selecting);
			while(rs.next()){
				int lid = rs.getInt("lid");
				String lname = rs.getString("lname");
				int population = rs.getInt("population");

				results += lid;
				results += ":" + lname;
				results += ":" + population;
				if (rs.isLast() == false){
					results += '#';
				}
			}
			rs.close();
			sql.close();
			return results;
		}catch(SQLException se){
			return "";
		}catch(Exception e){
			return "";
		}
	}
	
  
	public boolean updateHeight(int cid, int decrH){
		try {
	    	sql = connection.createStatement();
	    	String selecting = "UPDATE a2.country SET height = (height - " + decrH + ") WHERE cid = " + cid + ";";
	    	sql.executeUpdate(selecting);
	    	sql.close();
			return true;
		}catch(SQLException se){
			return false;
		}catch(Exception e){
			return false;
		}
	}
    
	public boolean updateDB(){
		try {
	    		sql = connection.createStatement();
	    		String deleting = "DROP TABLE IF EXISTS a2.mostPopulousCountries CASCADE;";
	    		sql.executeUpdate(deleting);
	    	
	    		String creating = "CREATE TABLE a2.mostPopulousCountries (cid INTEGER NOT NULL, cname VARCHAR(20) NOT NULL);";
	    		sql.executeUpdate(creating);
	    	
	    		String inserting = "INSERT INTO a2.mostPopulousCountries SELECT cid, cname FROM a2.country WHERE population > 100000000 ORDER BY cid;";
	    		sql.executeUpdate(inserting);
	    		sql.close();
			return true;
		}catch(SQLException se){
			return false;
		}catch(Exception e){
			return false;
		}
	}
  
}


