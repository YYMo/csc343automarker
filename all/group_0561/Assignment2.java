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
		} catch (Exception e) {
			return;
		}
	}
	
	//Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password){
		try {
			connection = DriverManager.getConnection(URL, username, password);
			Statement statement = connection.createStatement();
			try {
				statement.execute("set search_path to A2");
			} finally {
				statement.close();
			}
		} catch (SQLException e) {
			return false;
		}
		return (connection != null);
	}
	
	//Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB(){
		try {
			connection.close();
			return true;
		} catch (Exception e) {
			return false;
		}
	}
	
	public boolean insertCountry (int cid, String name, int height, int population) {
		try {
			String sqlText = "INSERT INTO country (cid, cname, height, population) "+
				"VALUES (?, ?, ?, ?);";
			ps = connection.prepareStatement(sqlText);
			ps.setInt(1, cid);
			ps.setString(2, name);
			ps.setInt(3, height);
			ps.setInt(4, population);
			int insertednum = ps.executeUpdate();
			ps.close();
			return (insertednum == 1);
		} catch (SQLException e) {
			return false;
		}
	}
	
	public int getCountriesNextToOceanCount(int oid) {
		try {
			String sqlText = "SELECT count(*) as num " +
				"FROM oceanAccess "+
				"WHERE oid = ?;";
			ps = connection.prepareStatement(sqlText);
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			int count = 0;
			if(rs != null){
				while (rs.next()) {
					count = rs.getInt("num");
				}
			}
			rs.close();
			ps.close();
			return count;
		} catch (SQLException e) {
			return -1;
		}
	}
	 
	public String getOceanInfo(int oid){
		try {
			String sqlText = "SELECT * " +
				"FROM ocean "+
				"WHERE oid = ?;";
			ps = connection.prepareStatement(sqlText);
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			String oceaninfo = "";
			if(rs != null){
				while (rs.next()) {
					oceaninfo = rs.getInt("oid") +
						":" + rs.getString("oname") +
						":" + rs.getInt("depth");
				}
			}
			rs.close();
			ps.close();
			return oceaninfo;
		} catch (SQLException e) {
			return "";
		}
	}

	public boolean chgHDI(int cid, int year, float newHDI){
		try {
			String sqlText = "UPDATE hdi "+
				"SET hdi_score = ?" +
				"WHERE cid = ? and year = ?;";
			ps = connection.prepareStatement(sqlText);
			ps.setFloat(1, newHDI);
			ps.setInt(2, cid);
			ps.setInt(3, year);
			int updatednum = ps.executeUpdate();
			ps.close();
			return (updatednum == 1);
		} catch (SQLException e) {
			return false;
		}
	}

	public boolean deleteNeighbour(int c1id, int c2id){
		try {
			String sqlText = "DELETE FROM neighbour "+
				"WHERE neighbor = ? and country = ?;";
			ps = connection.prepareStatement(sqlText);
			//delete (a,b)
			ps.setInt(1, c1id);
			ps.setInt(2, c2id);
			int deletednum = ps.executeUpdate();
			//delete (b,a)
			ps.setInt(1, c2id);
			ps.setInt(2, c1id);
			deletednum += ps.executeUpdate();
			ps.close();
			return (deletednum == 2);
		} catch (SQLException e) {
			return false;
		}
	}

	public String listCountryLanguages(int cid){
		try {
			String languages = "";
			String sqlText = "SELECT lid, lname, round(c.population*l.lpercentage/100) as pop " +
				"FROM country as c, language as l " +
				"WHERE c.cid=? and l.cid = c.cid " +
				"ORDER BY pop DESC;";
			ps = connection.prepareStatement(sqlText);
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			if(rs != null) {
				while (rs.next()) {
					languages = languages + rs.getInt("lid") + ":" +
						rs.getString("lname") + ":" +
						rs.getInt("pop") + "#";
				}
			}
			rs.close();
			ps.close();
			//remove last pound sign
			languages = languages.substring(0, languages.length() - 1);
			return languages;
		} catch (SQLException e) {
			return "";
		}
	}
	
	public boolean updateHeight(int cid, int decrH){
		try {
			String sqlText = "UPDATE country "+
				"SET height = height - ? " +
				"WHERE cid = ?;";
			ps = connection.prepareStatement(sqlText);
			ps.setInt(1, decrH);
			ps.setInt(2, cid);
			ps.executeUpdate();
			ps.close();
			return true;
		} catch (SQLException e) {
			return false;
		}
	}

	public boolean updateDB(){
		try{
			sql = connection.createStatement();
			sql.executeUpdate("DROP TABLE IF EXISTS mostPopulousCountries;");
			sql.executeUpdate("CREATE TABLE mostPopulousCountries" +
				"(cid INTEGER," +
				"cname VARCHAR(20));");
			String sqlText = "INSERT INTO mostPopulousCountries ( "+
				"SELECT cid, cname " +
				"FROM country " +
				"WHERE population > 100000000" +
				"ORDER BY cid ASC );";
			sql.executeUpdate(sqlText);
			sql.close();
			return true;
		} catch (SQLException e) {
			return false;
		}
	}
}
