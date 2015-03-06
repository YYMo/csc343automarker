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
  
  //To prepare statements
  String queryString;
  String updateString;
  
  //CONSTRUCTOR
  Assignment2() throws ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is successful
  public boolean connectDB(String URL, String username, String password){
                try {
                        connection = DriverManager.getConnection(URL, username, password);
                        return true;
                }
                catch (SQLException se){
                        return false;
        }
  }
  
  //Closes the connection. Returns true if closure was successful
  public boolean disconnectDB(){
                try {
                connection.close();
                return connection.isClosed();
                }
                catch (SQLException se) {
                        return false;
                }
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
        try {
                updateString = "INSERT INTO a2.country VALUES(?, ?, ?, ?);";
                ps = connection.prepareStatement(updateString);
                ps.setInt(1, cid);
                ps.setString(2, name);
                ps.setInt(3, height);
                ps.setInt(4, population);
                if (ps.executeUpdate() == 0) {
                                return false;
                        }
                return true;
        }
        //The DBMS will itself return an exception whenever a unique violation occurs.
        catch (SQLException se) {
               // SQLError(se);
                return false;
        }
 }
  
  public int getCountriesNextToOceanCount(int oid) {
        
        try {
                queryString = "SELECT COUNT(*) FROM a2.oceanAccess WHERE oid = ?;";
                ps = connection.prepareStatement(queryString);
                ps.setInt(1, oid);
                rs = ps.executeQuery();
                
                //Since we want an aggregate value, the first row contains all the info we need.
                rs.next();
                int result = rs.getInt(1);
                rs.close();
                return result;
        }
        
        catch (SQLException se) {
               // SQLError(se);
                return -1;
        }
  }
   
  public String getOceanInfo(int oid) throws SQLException {
        queryString = "SELECT * FROM a2.ocean WHERE oid = ?;";
        ps = connection.prepareStatement(queryString);
        ps.setInt(1, oid);
        rs = ps.executeQuery();
        //Since the query will return an empty relation if the given ocean does not exist.
        try{
                if (rs.next() == false) {
                return "";
        }
        else {
                return Integer.toString(rs.getInt(1)) + ":" + rs.getString(2) + ":" + Integer.toString(rs.getInt(3)); 
        }
        }
        
        catch (SQLException se) {
                //SQLError(se);
                return null;
        }
        
 }

  public boolean chgHDI(int cid, int year, float newHDI){
	try {
		updateString =  "UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? AND year = ?;";
		ps = connection.prepareStatement(updateString);
		ps.setFloat(1, newHDI);
		ps.setInt(2, cid);
		ps.setInt(3, year);
		if (ps.executeUpdate() == 0) {
						return false;
				}
		return true;
   }
   
   catch (SQLException se) {
        return false;
		}
  }

  public boolean deleteNeighbour(int c1id, int c2id){
   try {
		//Delete one tuple representing the neighbour relationship
		updateString = "DELETE FROM a2.neighbour WHERE country = ? AND neighbor = ?;";
		ps = connection.prepareStatement(updateString);
		ps.setInt(1, c1id);
		ps.setInt(2, c2id);
		if (ps.executeUpdate() == 0) {
						return false;
		}
		
		//Delete the pseudo duplicate
		ps.setInt(1, c2id);
		ps.setInt(2, c1id);
		if (ps.executeUpdate() == 0) {
						return false;
		}
		return true;
   }    
   catch (SQLException se) {
        return false;
		}
  }
  
  public String listCountryLanguages(int cid) throws SQLException {
  
        queryString = "SELECT lid, cname, (lpercentage / 100) * population AS  population FROM a2.language, a2.country WHERE country.cid = language.cid AND country.cid = ? ORDER BY population;";
        ps = connection.prepareStatement(queryString);
        ps.setInt(1, cid);
        rs = ps.executeQuery();
        String resultString = "";
        
        //Check if the SQL query results was empty.
        try {if (rs.next() == false) {
                return resultString;
        }
        
        //Iterate over the results set, extracting the required info, checking whether to add the pound symbol if not on the last row.
        while (rs.next()) {
			if (rs.isLast() == false) {
					resultString = resultString + Integer.toString(rs.getInt(1)) + ":" + rs.getString(2) + ":" + Integer.toString(rs.getInt(3)) + "#";
			}
			else {
					resultString = resultString + Integer.toString(rs.getInt(1)) + ":" + rs.getString(2) + ":" + Integer.toString(rs.getInt(3));
			}
        }               
        return resultString;
        }
        catch (SQLException se) {
                //SQLError(se);
                return null;
        }
  }
  
  public boolean updateHeight(int cid, int decrH){
    try {
		updateString = "UPDATE a2.country SET height = height - ? WHERE cid = ?;";
		ps = connection.prepareStatement(updateString);
		ps.setInt(1, decrH);
		ps.setInt(2, cid);
		if (ps.executeUpdate() == 0) {
						return false;
				}
		return true;
	}

	catch (SQLException se) {
	   // SQLError(se);
		return false;
	}
 }
   
	public boolean updateDB(){
		try {
				updateString = "CREATE TABLE a2.mostPopulousCountries AS (SELECT cid, cname FROM a2.country WHERE population;";
				sql = connection.createStatement();
				sql.executeUpdate(updateString);
				return true;
		}

		catch (SQLException se) {
				return false;
		} 
	} 
}