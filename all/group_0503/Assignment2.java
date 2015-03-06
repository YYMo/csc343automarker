// csc343 - a2 
// Shivain Thapar - g3thapar
// Szymon Stopyra - g3stopyr

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

	Assignment2() {

		try { //load JDBC Driver
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) { }
	}
  
  	//Using the input parameters, establish a connection to be used for this session. Returns true if connection is successful.
  	public boolean connectDB(String URL, String username, String password) {

  		boolean ans = false;

		try { //Open connection
			connection = DriverManager.getConnection(URL, username, password);
		} catch (SQLException e) { 
			return false;
		}

		if (connection != null) { //Connection Successful 
			ans = true;
		}

		return ans;
	}
  
  	//Closes the connection. Returns true if closure was sucessful
  	public boolean disconnectDB()	{
		
		boolean ans = false;

		if (connection != null) {
			try {
				connection.close();
				ans = true;
			} catch (SQLException e) {
				return false;
			}
		}
		return ans;
	}
	
  	public boolean insertCountry (int cid, String name, int height, int population) {

  		boolean ans = false;

		try{
			sql = connection.createStatement();
			
			String sqlText1 = "SELECT * FROM a2.country WHERE cid = " + Integer.toString(cid);

			rs = sql.executeQuery(sqlText1);
			
			if (rs != null) {

				if (rs.next()){
					rs.close();
				} else {

					String sqlText2 = "INSERT INTO a2.country " +
									  "VALUES (?,?,?,?)	 " ;
					ps = connection.prepareStatement(sqlText2);

					ps.setInt(1,cid);
					ps.setString(2,name);
					ps.setInt(3,height);
					ps.setInt(4,population);

					ps.executeUpdate(); //Execute Insert 

					ans = true;

					ps.close();
				}
			}
		} catch (SQLException e) { return false; }

		return ans;
  	}
  
  	public int getCountriesNextToOceanCount(int oid) {

  		int count = -1; 

  		try{
			
			sql = connection.createStatement();

  			String sqlText = "SELECT * FROM a2.oceanAccess WHERE oid = " + Integer.toString(oid);

			rs = sql.executeQuery(sqlText);

			if (rs != null) {
				if (rs.next()){
					count = 1;
					while (rs.next()) {
						count = count + 1;
					}
				} 
			}
		} catch (SQLException e) { return -1; }

		return count;
	}

  	public String getOceanInfo(int oid) {
		
		String answer = "";

		try{
			sql = connection.createStatement();

			String sqlText1 = "SELECT * FROM a2.ocean WHERE oid = " + Integer.toString(oid);
			String oname = "";
			int depth = 0;

			rs = sql.executeQuery(sqlText1);

			if (rs != null) {
				if (rs.next()) {
					oname = rs.getString("oname");
					depth = rs.getInt("depth");
					rs.close();
					answer = Integer.toString(oid) + ":" + oname + ":" + Integer.toString(depth);
				} 
			}
  		} catch (SQLException e) { return ""; }

  		return answer;
  	}

  	public boolean chgHDI(int cid, int year, float newHDI){
	
		boolean ans = false;

		try {
			sql = connection.createStatement();

			String sqlText1 = "SELECT * FROM a2.hdi WHERE cid = " + Integer.toString(cid) + 
							  " and year = " + Integer.toString(year);

			rs = sql.executeQuery(sqlText1);

			if (rs != null) {
				if (rs.next()){

					String sqlText = "UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? AND year = ? ";

					ps = connection.prepareStatement(sqlText);

					ps.setFloat(1,newHDI);
					ps.setInt(2,cid);
					ps.setInt(3,year);

					ps.executeUpdate();
					ans = true; 
					ps.close();

				}
				rs.close();
			} 
		} catch (SQLException e) {return false;}

		return ans;
	}

  	public boolean deleteNeighbour(int c1id, int c2id){
		
		boolean ans = false;

		try{
			sql = connection.createStatement();

			String sqlText = "DELETE FROM a2.neighbour WHERE (neighbor = ? or neighbor = ?) and (country = ? or country = ?)";

			ps = connection.prepareStatement(sqlText);

			ps.setInt(1,c1id);
			ps.setInt(2,c2id);
			ps.setInt(3,c1id);
			ps.setInt(4,c2id);

			ps.executeUpdate(); //Execute Update
			ans = true; 

			ps.close();

		} catch (SQLException e) { return false; }

		return ans;
  	}
  
  	public String listCountryLanguages(int cid){

  		String answer = "";

		try{
			sql = connection.createStatement();

			String sqlText1 = 
						"SELECT LA.lid, LA.lname , (CO.population * LA.lpercentage) as population " + 
						"FROM a2.country CO JOIN a2.language LA ON CO.cid = LA.cid " +
						"WHERE CO.cid = " + Integer.toString(cid) +
						" ORDER BY population ;";

			int lid = 0;
			String lname = "";
			int population = 0;

			rs = sql.executeQuery(sqlText1);

			if (rs != null) {
				while (rs.next()){

					lid = rs.getInt("lid");
					lname = rs.getString("lname");
					population = rs.getInt("population");
					
					if (answer.equals("")){
						answer = answer + Integer.toString(lid) + ":" + lname + ":" + Integer.toString(population) ;
					} else {
						answer = answer.concat("#");
						answer = answer + Integer.toString(lid) + ":" + lname + ":" + Integer.toString(population);
					}
				}
				rs.close();
			}

		} catch (SQLException e) {  return ""; }

		return answer;
  	}
 
  	public boolean updateHeight(int cid, int decrH){
		
		boolean ans = false;
		
		try{
			sql = connection.createStatement();

			String sqlText1 = "SELECT * FROM a2.country WHERE cid = " + Integer.toString(cid);

			rs = sql.executeQuery(sqlText1);

			if (rs != null) {
				if (rs.next()) {

					int height = rs.getInt("height");
					int newheight = height - decrH;

					String sqlText = "UPDATE a2.country SET height = ? WHERE cid = ?";

					ps = connection.prepareStatement(sqlText);

					ps.setInt(1,newheight);
					ps.setInt(2,cid);

					ps.executeUpdate(); //Execute Update
					ans = true;

					ps.close();
				}
				rs.close();
  			}
  		} catch (SQLException e) { return false; }

  		return ans;
  	}
	
  	public boolean updateDB(){

  		boolean ans = false;
		
		try{

			sql = connection.createStatement();

			String sqlTable = "CREATE TABLE a2.mostPopulousCountries (cid	INTEGER	NOT NULL,cname 	VARCHAR(20)	NOT NULL);" ;
			
			sql.executeUpdate(sqlTable); 

			String sqlText = "SELECT * FROM a2.country WHERE population > 100000000 ORDER BY cid;";
			rs = sql.executeQuery(sqlText);

			if (rs != null) {
				while (rs.next()) {

					int cid = rs.getInt("cid");
					String cname = rs.getString("cname");

					String sqlInsert = "INSERT INTO a2.mostPopulousCountries VALUES (?, ?);" ;

					ps = connection.prepareStatement(sqlInsert);

					ps.setInt(1,cid);
					ps.setString(2,cname);

					ps.executeUpdate(); //Execute Update

					ps.close();
				}
				ans = true;
				rs.close();
  			}
  		} catch (SQLException e) { return false; }
  		
  		return ans;
  	}
}