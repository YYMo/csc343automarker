package sql;

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

			// Load JDBC driver
			Class.forName("org.postgresql.Driver");

		} catch (ClassNotFoundException e) {

			System.out.println("Where is your PostgreSQL JDBC Driver? Include in your library path!");
			e.printStackTrace();
			return;
		}

		System.out.println("PostgreSQL JDBC Driver Registered!");
	}

	//Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password){
		try {

			String connectionAddress = "jdbc:postgresql://localhost:5432/csc343h-" + username;
			connection = DriverManager.getConnection(connectionAddress, username, password);

		} catch (SQLException e) {

			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return false;

		}

		if (connection != null) {
			System.out.println("You made it, take control of your database now!");
			return true;
		} else {
			System.out.println("Failed to make connection!");
			return false;
		}
	}

	//Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB(){
		try{
			connection.close();
			return true;
		} catch (SQLException e){
			return false;
		}
	}

	public boolean insertCountry (int cid, String name, int height, int population) {
		try{

			//Create a Statement for executing SQL queries
			sql = connection.createStatement();

			String sqlText;

			sqlText = "INSERT INTO a2.country " +
			"VALUES ("+ cid +", '"+ name +"', "+ height +", "+ population +")";

			sql.executeUpdate(sqlText);
			//System.exit(1);


		} catch (SQLException e) {

			System.out.println("Query Exection Failed!");
			e.printStackTrace();
			return false;

		}
		return true;
	}

	public int getCountriesNextToOceanCount(int oid) {
		try{

			sql = connection.createStatement();

			String sqlText;

			sqlText = "SELECT COUNT(cid) AS totalCountries " +
			"FROM a2.oceanaccess " +
			"GROUP BY oid " +
			"HAVING oid = " + Integer.toString(oid);


			rs = sql.executeQuery(sqlText);
			
			int count=0;
			while (rs.next()){
				count = rs.getInt("totalCountries");
			}
			//Close the resultset
			rs.close();


			return count;
		} catch (SQLException e) {

			System.out.println("Query Exection Failed!");
			e.printStackTrace();
			return -1;

		}

	}

	public String getOceanInfo(int oid){

		try{  
			sql = connection.createStatement();

			String sqlText;

			sqlText = "SELECT oid, oname, depth " +
			"FROM a2.ocean "+
			"WHERE oid = " + oid;

			rs = sql.executeQuery(sqlText);
			String result = "";
			while (rs.next()){
				result = Integer.toString(rs.getInt(1))
				+ ":"+ rs.getString(2)
				+ ":"+ Integer.toString(rs.getInt(3));
			}
			rs.close();

			return result;
		} catch (SQLException e) {

			System.out.println("Query Exection Failed!");
			e.printStackTrace();
			return "";

		}
	}

	public boolean chgHDI(int cid, int year, float newHDI){
		try{

			String sqlText;


			sqlText = "UPDATE a2.hdi " +
			"SET hdi_score = ? " +
			"WHERE cid = ? AND year = ?";
			PreparedStatement sqlStmt = connection.prepareStatement(sqlText);

			sqlStmt.setFloat(1, newHDI);
			sqlStmt.setInt(2, cid);
			sqlStmt.setInt(3, year);

			sqlStmt.executeUpdate();

			return true;

		} catch (SQLException e) {

			System.out.println("Query Exection Failed!");
			e.printStackTrace();
			return false;

		}
	}

	public boolean deleteNeighbour(int c1id, int c2id){
		try{
			String sqlText;


			sqlText = "DELETE FROM a2.neighbour " + 
			"WHERE country = ? AND neighbor = ? " +
			"OR country = ? AND neighbor = ?";
			PreparedStatement sqlStmt = connection.prepareStatement(sqlText);

			sqlStmt.setInt(1, c1id);
			sqlStmt.setInt(2, c2id);
			sqlStmt.setInt(3, c2id);
			sqlStmt.setInt(4, c1id);

			sqlStmt.executeUpdate();
			return true;

		} catch (SQLException e) {

			System.out.println("Query Exection Failed!");
			e.printStackTrace();
			return false;

		}
	}

	public String listCountryLanguages(int cid){
		try{
			sql = connection.createStatement();

			String sqlText;

			sqlText = "SELECT lid, lname, ROUND(lpercentage*population) AS lpop " +
			"FROM a2.language JOIN a2.country " +
			"ON a2.language.cid = a2.country.cid " +
			"WHERE a2.language.cid = "+ Integer.toString(cid);

			rs = sql.executeQuery(sqlText);

			String result = "";
			while (rs.next()) {
				result += rs.getInt("lid")
				+":" + rs.getString("lname")
				+":" + rs.getInt("lpop");
				result += "#";
			}
			//Close the resultset
			rs.close();

			return result;
		} catch (SQLException e) {

			System.out.println("Query Exection Failed!");
			e.printStackTrace();
			return "";

		}
	}

	public boolean updateHeight(int cid, int decrH){
		try{
			String sqlText;
			// is decrH the decrement or the new decreased height?

			sqlText = "UPDATE a2.country " +
			"SET height = ? " +
			"WHERE cid = ?";
			PreparedStatement sqlStmt = connection.prepareStatement(sqlText);

			sqlStmt.setInt(1, decrH);
			sqlStmt.setInt(2, cid);


			sqlStmt.executeUpdate();
			return true;

		} catch (SQLException e) {

			System.out.println("Query Exection Failed!");
			e.printStackTrace();
			return false;

		}
	}

	public boolean updateDB(){
		try{
			sql = connection.createStatement();

			String sqlText;

			sqlText = "CREATE TABLE a2.mostPopulousCountries AS " +
			"SELECT cid, cname "+
			"FROM a2.country " +
			"WHERE population >= 10000000 " +
			"ORDER BY cid ASC";

			rs = sql.executeQuery(sqlxt);

			//Close the resultset
			rs.close();

			return true;


		} catch (SQLException e) {

			System.out.println("Query Exection Failed!");
			e.printStackTrace();
			return false;

		}
	}

}