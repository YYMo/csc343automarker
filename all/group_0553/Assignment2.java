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


  //Main method
  public static void  main(String[] args){

     Assignment2  a2 = new Assignment2();
     
     //Check q1.
     System.out.println("Connection established: " + a2.connectDB("jdbc:postgresql://localhost:5432/csc343h-c2laraic", "c2laraic", ""));
	
	//Check q2.
	//System.out.println("Connection closed: " + a2.disconnectDB());
	
	 //Check q3.
     System.out.println("Insert country: " + a2.insertCountry(260, "India", 7190, 1013662));
	
	//Check q4.
	//System.out.println("Get num of countries next to ocean: " + a2.getCountriesNextToOceanCount(1));
  
	//Check q5.
	//System.out.println("Ocean info: " + a2.getOceanInfo(1));
  	
	//check q6.
	//System.out.println("Update height: " + a2.updateHeight(1, 1000));
	//check q7.
	System.out.println("Delete neighbour: " + a2.deleteNeighbour(1, 12)); 
	//Check q10.
	//System.out.println("UpdateDB: " + a2.updateDB());
  }

  //CONSTRUCTOR
  Assignment2(){
	try {
		
      Class.forName("org.postgresql.Driver");
      
	} catch (ClassNotFoundException e) {
		System.out.println("Where is your PostgreSQL JDBC Driver? Include in your library path!");
		e.printStackTrace();
		return;
	}
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try {
		        // A connection to the database 
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
		} catch (SQLException e) {

			System.out
					.println("An error occured and connection could not be closed.");
			return false;
		}

		return true;   
  }
  

  public boolean insertCountry (int cid, String name, int height, int population) {
		
		boolean countryExist = false;
		
		try{
			sql = connection.createStatement();
			
			String sqlCountries = "select * from a2.country";
			
			rs = sql.executeQuery(sqlCountries);
			
			while (rs.next()) {
				
			//check if it exist and set countryExist to true.
				if (rs.getInt("cid") == cid) {
					countryExist = true;
				}
			}
			if (countryExist == false) {
				String sqlText = "insert into a2.country values (?, ?, ?, ?) ";
				//prep statement
				ps = connection.prepareStatement(sqlText);
				
				ps.setInt(1, cid);
				ps.setString(2, name);
				ps.setInt(3, height);
				ps.setInt(4, population);
				
				ps.executeUpdate();

				return true;
			}
		} catch (SQLException ex){
			 System.err.println("SQL Exception." +
                        "<Message>: " + ex.getMessage());
		}
		return false;
  }

  public int getCountriesNextToOceanCount(int oid) {
	  		
	  	try {
			sql = connection.createStatement();
			String sqlCountries = "select count(cid) from a2.oceanaccess where oid=" + oid + ";";
			
			rs = sql.executeQuery(sqlCountries);

			rs.next();
			 
			int value =  rs.getInt(1);
			rs.close();
			sql.close();
			return value;
			
		} catch (SQLException ex){
			 System.err.println("SQL Exception." +
                        "<Message>: " + ex.getMessage());
		}
	return -1;  
  }
   
  public String getOceanInfo(int oid){
	  
	 try {
     	sql = connection.createStatement();

	 String sqlCountries = "select * from a2.ocean where oid=" + oid + ";";
	
	 rs = sql.executeQuery(sqlCountries);

	 rs.next();
	
	 int val = rs.getInt("oid");
	 
	 String oname = rs.getString("oname");
	 int depth = rs.getInt("depth");
	 
	rs.close();
	sql.close();
		
	 return val + ":" + oname + ":" + depth;
	 } catch (SQLException ex){
				 System.err.println("SQL Exception." +
							"<Message>: " + ex.getMessage());
			}
				
	   return "";
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	  
	  //select * from hdi where cid = 1 and year = 2000;
	  
	  try {
     sql = connection.createStatement();

	 String sqlCountries = "select * from a2.ocean where oid=" + cid + ";";
	
	 rs = sql.executeQuery(sqlCountries);

	 rs.next();
	
	 int val = rs.getInt("oid");
	 
	 String oname = rs.getString("oname");
	 int depth = rs.getInt("depth");
	 
	rs.close();
	sql.close();
		
	 return true;
	 } catch (SQLException ex){
				 System.err.println("SQL Exception." +
							"<Message>: " + ex.getMessage());
	 }

   return false;
  }

public boolean deleteNeighbour(int c1id, int c2id){

	try {
		 sql = connection.createStatement();

		String  query1 = "DELETE FROM neighbour" +
		"WHERE country=" + c1id;

		String query2 = "DELETE FROM neighbour" +
		"WHERE country=" + c2id;

		sql.executeUpdate(query1);
		sql.executeUpdate(query2);
		sql.close();

		}catch (SQLException e){
			return false;

	}
	return true;
}
  
  public String listCountryLanguages(int cid){
	return "";
  }


public boolean updateHeight(int cid, int decrH){

	try {

		String query1 = "SELECT height FROM a2.country " +
		"WHERE cid=?";

		ps = connection.prepareStatement(query1);

		ps.setInt(1, cid);
		
		rs = ps.executeQuery();
		
		rs.next();

		int value = rs.getInt("height") - decrH;

		String query2 = "update a2.country set height = ? where cid=? ";


		ps = connection.prepareStatement(query2);

		ps.setInt(1, value);
		ps.setInt(2, cid);

		ps.executeUpdate();

		return true;

	}catch (SQLException e){
		return false;
	}

}
public boolean updateDB(){
	
	try {
		int numberOfRows;
		
		String dropSqlTable = "DROP TABLE IF EXISTS a2.mostPopulousCountries CASCADE";
		String createTableSQL = "CREATE TABLE a2.mostPopulousCountries("
		+ "cid INTEGER NOT NULL, "
		+ "cname VARCHAR(20) NOT NULL, "
		+ "PRIMARY KEY (cid) "
		+ ")";
		
		sql = connection.createStatement();
		sql.executeUpdate(dropSqlTable);
		sql.executeUpdate(createTableSQL);
		
		String insertTableSQL = "INSERT INTO a2.mostPopulousCountries (cid, cname) (SELECT cid, cname " +
		"FROM a2.country where population > 100000000 ORDER BY cid ASC)";
		// String insertTableSQL = "INSERT INTO a2.mostPopulousCountries (cid, cname)" + rs;
		numberOfRows = sql.executeUpdate(insertTableSQL);
		
		if (numberOfRows > 0){
			sql.close();
			return true;
		}else
		return false;
	}catch(SQLException e) {
		System.err.println("SQL Exception in updateDB." + e.getMessage());
	}
	return false;
	}
}
