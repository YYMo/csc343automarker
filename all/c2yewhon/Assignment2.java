import java.sql.*;
import java.util.ArrayList;
import java.util.List;

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
			Class.forName("org.postgresql.Driver");
		}
		catch (ClassNotFoundException e) {
			System.out.println("Failed to find the JDBC driver");
			return false;
		}
		try {
			connection = DriverManager.getConnection(URL, username, password);
			PreparedStatement pStatement;
			String queryString;
			queryString = "SET search_path TO A2";
			pStatement = connection.prepareStatement(queryString);
			pStatement.executeUpdate();
			pStatement.close();
		}
		catch (SQLException se)
		{
			System.err.println("SQL Exception." +
					"<Message>: " + se.getMessage());
			return false;
		}
		return true;
	}

	//Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB(){
		try {
			connection.close();
		} catch (SQLException e) {
			System.err.println("SQL Exception." +
					"<Message>: " + e.getMessage());
			return false;
		}
		return true;    
	}

	public boolean insertCountry (int cid, String name, int height, int population) {
		PreparedStatement pStatement;
		String queryString;
		queryString = "INSERT INTO country values(?, ?, ?, ?)";
		try {
			pStatement = connection.prepareStatement(queryString);
			pStatement.setInt(1, cid);
			pStatement.setString(2, name);
			pStatement.setInt(3, height);
			pStatement.setInt(4, population);
			pStatement.executeUpdate();
			pStatement.close();
		} catch (SQLException e) {
			System.err.println("SQL Exception." +
					"<Message>: " + e.getMessage());
			return false;
		}
		return true;
	}

	public int getCountriesNextToOceanCount(int oid) {
		PreparedStatement pStatement;
		ResultSet rs;
		String queryString;
		queryString = "SELECT COUNT(*) count FROM oceanAccess WHERE oid=?";
		int ans = -1;
		try {
			pStatement = connection.prepareStatement(queryString);
			pStatement.setInt(1, oid);
			rs = pStatement.executeQuery();
			if (rs.next()) ans = rs.getInt("count");
			rs.close();
			pStatement.close();
		} catch (SQLException e) {
			System.err.println("SQL Exception." +
					"<Message>: " + e.getMessage());
			return -1;
		}
		return ans;
	}

	public String getOceanInfo(int oid){
		PreparedStatement pStatement;
		ResultSet rs;
		String queryString;
		queryString = "SELECT * FROM ocean WHERE oid=?";
		String ans = "";
		try {
			pStatement = connection.prepareStatement(queryString);
			pStatement.setInt(1, oid);
			rs = pStatement.executeQuery();
			if (rs.next()) {
				String name = rs.getString("oname");
				int depth = rs.getInt("depth");
				ans = oid + ":" + name + ":" + depth;
			}
			rs.close();
			pStatement.close();
		} catch (SQLException e) {
			System.err.println("SQL Exception." +
					"<Message>: " + e.getMessage());
			return "";
		}
		return ans;
	}

	public boolean chgHDI(int cid, int year, float newHDI){
		PreparedStatement pStatement;
		String queryString;
		queryString = "UPDATE hdi SET hdi_score=? WHERE cid=? AND year=?";
		int row = -1;
		try {
			pStatement = connection.prepareStatement(queryString);
			pStatement.setFloat(1, newHDI);
			pStatement.setInt(2, cid);
			pStatement.setInt(3, year);
			row = pStatement.executeUpdate();
			pStatement.close();
		} catch (SQLException e) {
			System.err.println("SQL Exception." +
					"<Message>: " + e.getMessage());
			return false;
		}
		return (row > 0);
	}

	public boolean deleteNeighbour(int c1id, int c2id){
		PreparedStatement pStatement1, pStatement2;
		String queryString;
		queryString = "DELETE FROM neighbour WHERE country=? AND neighbor=?";
		try {
			pStatement1 = connection.prepareStatement(queryString);
			pStatement2 = connection.prepareStatement(queryString);
			pStatement1.setInt(1, c1id);
			pStatement1.setInt(2, c2id);
			pStatement2.setInt(1, c2id);
			pStatement2.setInt(2, c1id);
			pStatement1.executeUpdate();
			pStatement2.executeUpdate();
			pStatement1.close();
			pStatement2.close();
		} catch (SQLException e) {
			System.err.println("SQL Exception." +
					"<Message>: " + e.getMessage());
			return false;
		}
		return true;
	}

	class CountryLanguage{
		public CountryLanguage(int lid, String name, int population) {
			this.lid = lid;
			this.name = name;
			this.population = population;
		}
		public int lid;
		public String name;
		public Integer population;

		public String toString() {
			return lid + ":" + name + ":" + population;
		}
	}

	public String listCountryLanguages(int cid){
		PreparedStatement pStatement, cStatement;
		ResultSet rs, cs;
		String queryString, countryQueryString;
		queryString = "SELECT * FROM language WHERE cid=? ORDER BY lpercentage DESC";
		countryQueryString = "SELECT * FROM country WHERE cid=?";
		String ans = "";
		List<CountryLanguage> lList = new ArrayList<CountryLanguage>();
		int rawPop;
		try {
			pStatement = connection.prepareStatement(queryString);
			cStatement = connection.prepareStatement(countryQueryString);
			pStatement.setInt(1, cid);
			cStatement.setInt(1, cid);
			rs = pStatement.executeQuery();
			cs = cStatement.executeQuery();
			if (cs.next()) {
				rawPop = cs.getInt("population");
			} else {
				return "";
			}
			cs.close();
			while (rs.next()) {
				int i = (int) (rs.getFloat("lpercentage") * rawPop);
				CountryLanguage l = new CountryLanguage(rs.getInt("lid"),
						rs.getString("lname"), i);
				lList.add(l);
			}
			rs.close();
			pStatement.close();
			cStatement.close();
			StringBuilder sb = new StringBuilder();
			for (CountryLanguage l : lList) {
				sb.append(l.toString());
				sb.append("#");
			}
			sb.deleteCharAt(sb.length() - 1);
			ans = sb.toString();
		} catch (SQLException e) {
			System.err.println("SQL Exception." +
					"<Message>: " + e.getMessage());
			return "";
		}
		return ans;
	}

	public boolean updateHeight(int cid, int decrH){
		PreparedStatement pStatement, cStatement;
		String queryString, countryQueryString;
		ResultSet cs;
		queryString = "UPDATE country SET height=? WHERE cid=?";
		countryQueryString = "SELECT height FROM country WHERE cid=?";
		int height, row = -1;
		try {
			pStatement = connection.prepareStatement(queryString);
			pStatement.setInt(2, cid);
			cStatement = connection.prepareStatement(countryQueryString);
			cStatement.setInt(1, cid);
			cs = cStatement.executeQuery();
			if (cs.next()) {
				height = cs.getInt("height");
			} else {
				return false;
			}
			cs.close();
			cStatement.close();
			pStatement.setInt(1, height - decrH);
			row = pStatement.executeUpdate();
			pStatement.close();
		} catch (SQLException e) {
			System.err.println("SQL Exception." +
					"<Message>: " + e.getMessage());
			return false;
		}
		return (row > 0);
	}

	public boolean updateDB(){
		PreparedStatement pStatement;
		String queryString;
		try {
			queryString = "DROP TABLE IF EXISTS mostPopulousCountries CASCADE";
			pStatement = connection.prepareStatement(queryString);
			pStatement.executeUpdate();
			pStatement.close();
			queryString = "CREATE TABLE mostPopulousCountries(" +
					"cid 		INTEGER 	PRIMARY KEY," +
					"cname 		VARCHAR(20)	NOT NULL)";
			pStatement = connection.prepareStatement(queryString);
			pStatement.executeUpdate();
			pStatement.close();
			queryString = "INSERT INTO mostPopulousCountries (SELECT cid, cname FROM country "
					+ "WHERE population>=100000000 ORDER BY cid ASC)";
			pStatement = connection.prepareStatement(queryString);
			pStatement.executeUpdate();
			pStatement.close();
		} catch (SQLException e) {	
			System.err.println("SQL Exception." +
					"<Message>: " + e.getMessage());
			return false;
		}
		return true;  
	}

}
