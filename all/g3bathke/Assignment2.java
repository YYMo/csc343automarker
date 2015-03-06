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
	Assignment2() throws ClassNotFoundException {
	    try {
			Class.forName("org.postgresql.Driver");
	    }
	    catch (ClassNotFoundException e) {}
	}
	
	//Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password) throws SQLException{
		try {
			connection = DriverManager.getConnection(URL,username,password);
		} catch (SQLException ex) {
			return false;
		}
		return true;
	}
	
	//NOT DONE
	//Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB() throws SQLException {
		try {
			connection.close();
			ps.close();
			rs.close();
		} catch (SQLException ex) {
			System.out.println(ex.getMessage());
			return false;
		}
		return true;
	}
	
	public boolean insertCountry (int cid, String name, int height, int population) throws SQLException {
		try {
			ps = this.connection.prepareStatement("SELECT cid FROM a2.country");
			rs = ps.executeQuery();
			while (rs.next()) {
				if (rs.getInt(1)==cid) {
					return false;
				}
			}
			ps = this.connection.prepareStatement("INSERT INTO a2.country(cid, cname, height, population) VALUES("+cid+",'"+name+"',"+height+","+population+")");
			rs = ps.executeQuery();
		} catch (SQLException ex) {
			return false;
		}
		return true;
	}
	
	public int getCountriesNextToOceanCount(int oid) {
		int i = -1;
		try {
			ps = this.connection.prepareStatement("SELECT cid FROM a2.oceanAccess WHERE oid="+oid);
			rs = ps.executeQuery();
			i=0;
			while (rs.next()) {
				i++;
            }
		} catch (SQLException ex) {
			return -1;
		}
		return i;
	}
	
	public String getOceanInfo(int oid){
		String oceanInfo = "";
		try {
			ps = this.connection.prepareStatement("SELECT * FROM a2.ocean WHERE oid="+oid);
			rs = ps.executeQuery();
			while (rs.next()) {
				oceanInfo = rs.getInt(1) + ":" + rs.getString(2) + ":" + rs.getInt(3);
			}
		} catch (SQLException ex) {
			return "";
		}
		return oceanInfo;
	}
	
	public boolean chgHDI(int cid, int year, float newHDI) {
		try {
			ps = this.connection.prepareStatement(
				"UPDATE a2.hdi SET hdi_score="+newHDI
				+" WHERE cid="+cid+" AND year="+year);
			ps.executeUpdate();
		} catch (SQLException ex) {
			System.out.println(ex.getMessage());
			return false;
		}
		return true;
	}
	
	public boolean deleteNeighbour(int c1id, int c2id) {
		try {
			ps = this.connection.prepareStatement(
				"DELETE FROM a2.neighbour WHERE (country="+c1id+
				" AND neighbor="+c2id+") OR (country="+c2id+
				" AND neighbor="+c1id+")");
			ps.executeUpdate();
		} catch (SQLException ex) {
			return false;
		}
		return true;        
	}
	
	public String listCountryLanguages(int cid){
		String languageInfo = "";
		try {
			ps = this.connection.prepareStatement(
				"SELECT lid,lname,SUM(lpercentage*population) AS population FROM a2.language,a2.country WHERE language.cid="+cid+
				"AND country.cid="+cid+" GROUP BY lid, lname ORDER BY SUM(lpercentage*population)");
			rs = ps.executeQuery();
			while (rs.next()) {
				if (rs.isLast()) {
					languageInfo += rs.getInt(1) + ":" + rs.getString(2) + ":" + rs.getFloat(3);
				} else {
					languageInfo += rs.getInt(1) + ":" + rs.getString(2) + ":" + rs.getFloat(3) + "#";
				}
			}
		} catch (SQLException ex) {
			return "";
		}
		return languageInfo;
	}
	
	public boolean updateHeight(int cid, int decrH){
		try {
			ps = this.connection.prepareStatement(
				"UPDATE a2.country SET height="+decrH
				+" WHERE cid="+cid);
			ps.executeUpdate();
		} catch (SQLException ex) {
			return false;
		}
		return true;
	}
	
	public boolean updateDB(){
		try {	
			ps = this.connection.prepareStatement(
				"CREATE TABLE IF NOT EXISTS a2.mostPopulousCountries (" +
				"cid INTEGER PRIMARY KEY,"+
				"cname VARCHAR(20) NOT NULL)");
			ps.executeUpdate();
			ps = this.connection.prepareStatement(
				"INSERT INTO a2.mostPopulousCountries" +
				" (SELECT cid,cname FROM a2.country WHERE population>100000000 ORDER BY cid ASC)");
			ps.executeUpdate();
		} catch (SQLException ex) {
			return false;    
		}
	return true;
	}

}
