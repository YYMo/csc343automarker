import java.sql.*;
import java.io.*;

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
		}
		catch (ClassNotFoundException e) {
			System.out.println("Failed to find the JDBC driver");
		}
	}

	//Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password){
		try {
			connection = DriverManager.getConnection(URL, username, password);
			return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return false;
		}

	}

	//Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB(){
		try {
			connection.close();
			return true;  
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return false;  
		}
		  
	}

	public boolean insertCountry (int cid, String name, int height, int population) {
		try {
			ps = connection.prepareStatement("INSERT INTO a2.country VALUES(?,?,?,?)");
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
		int count = 0;
		try {
			ps = connection.prepareStatement("SELECT count(cid) FROM a2.oceanAccess WHERE oid=? GROUP BY oid");
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			if (rs != null){
				while (rs.next()){
					count = rs.getInt("count");
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
		String data = "";
		try {
			ps = connection.prepareStatement("SELECT * FROM a2.ocean WHERE oid=?");
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			if (rs != null){
				while (rs.next()){
					data = rs.getString("oid") + ":" + rs.getString("oname") + ":" + rs.getString("depth");
				}
			}
			rs.close();
			ps.close();
			return data;
		} catch (SQLException e) {
			return "";  
		}
	}

	public boolean chgHDI(int cid, int year, float newHDI){
		try {
			ps = connection.prepareStatement("UPDATE a2.hdi SET hdi_score = ? WHERE cid=? AND year=?");
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
			ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE country=? AND neighbor=?");
			ps.setInt(1, c1id);
			ps.setInt(2, c2id);
			ps.executeUpdate();
			ps.close();
			ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE country=? AND neighbor=?");
			ps.setInt(1, c2id);
			ps.setInt(2, c1id);
			ps.executeUpdate();
			ps.close();
			return true;
		} catch (SQLException e) {
			return false;
		}       
	}

	public String listCountryLanguages(int cid){
		int rowCount = 0;

		String data = "";
		try {
			ps = connection.prepareStatement("SELECT l.lid AS lid, l.lname AS lname, l.lpercent AS lpercent, c.population AS population" +
					"FROM a2.language l, a2.country c " +
					"WHERE lcid=? AND c.cid=l.cid");
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			if (rs != null){
				while (rs.next()){
					if(rowCount != 0){
						data = data + "#";
					}
					rowCount++;
					data = rs.getString("lid") + ":" + rs.getString("lname") + ":" + (rs.getInt("lpercent")*rs.getInt("population")/100);
				}
			}
			rs.close();
			ps.close();
			return data;
		} catch (SQLException e) {
			return "";  
		}
	}

	public boolean updateHeight(int cid, int decrH){
		try {
			ps = connection.prepareStatement("UPDATE a2.country SET height = height - ? WHERE cid=?");
			ps.setInt(1, decrH);
			ps.setInt(2, cid);;
			ps.executeUpdate();
			ps.close();
			return true;
		} catch (SQLException e) {
			return false;
		}
	}

	public boolean updateDB(){
		String sqlText = "CREATE TABLE a2.mostPopulousCountries (" +
				"cid 		INTEGER 	PRIMARY KEY REFERENCES a2.country(cid) ON DELETE RESTRICT," +
				"cname 		VARCHAR(20)	NOT NULL" +
				"PRIMARY KEY(cid,cname))";
		
		String sqlText2 = "INSERT INTO a2.mostPopulousCountries (" +
				"SELECT p.cid AS cid, p.cname AS cname" +
				"FROM (SELECT cid, cname FROM a2.country WHERE population>100000000 ORDER BY population DESC LIMIT 10) p" +
				"ORDER BY p.cid ASC" +
				")";
		try {
			sql = connection.createStatement();
			sql.executeUpdate(sqlText);
			sql.close();
			sql = connection.createStatement();
			sql.executeUpdate(sqlText2);
			sql.close();
			return true;
		} catch (SQLException e) {
			return false;
		}  
	}

}
