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
	public void Assignment2(){
		try {
			Class.forName("postgresql-9.1-903.jdbc4.jar");
		} catch (ClassNotFoundException e) {
			// ?
		}
	}

	//Using the input parameters, establish a connection to be used for this
	//session. Returns true if connection is successful.
	public boolean connectDB(String URL, String username, String password){
		try {
			connection = DriverManager.getConnection
					(URL, username, password);
		} catch (SQLException e) {
			System.out.println(e);
			return false;
		}
		return true;
	}

	//Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB(){
		try {
			connection.close();
			ps.close();
			sql.close();
		} catch (SQLException e) {
			return false;
		}
		return true;
	}

	public boolean insertCountry (int cid, String name, int height, int population) {
		int new_size;
		try {
			ps = connection.prepareStatement("INSERT INTO " + 
					"a2.country(cid, cname, height, population) " +
					" VALUES (?, ?, ?, ?)");
			ps.setInt(1, cid);
			ps.setString(2, name);
			ps.setInt(3, height);
			ps.setInt(4, population);
			new_size = ps.executeUpdate();
			ps.close();
			if (new_size != 1) {
				return false;
			}
		}
		catch (SQLException e) {
			return false;
		}
		return true;
	}


	public int getCountriesNextToOceanCount(int oid) {
		int result = 0;
		try {
			ps = connection.prepareStatement("SELECT COUNT(cid) "
					+ "FROM a2.oceanAccess WHERE oid = ?");
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			if (rs.next()) {
				result = rs.getInt(1);
				ps.close();
			}
		} catch (SQLException e) {
			return -1;
		}
		return result;
	}

	public String getOceanInfo(int oid){
		String result = "";
		try {
			ps = connection.prepareStatement("SELECT * FROM a2.ocean WHERE oid = ?");
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			if (rs.next()) {
				result += Integer.toString(rs.getInt(1)) + ":" +
						rs.getString(2) + ":" +
						Integer.toString(rs.getInt(3));
			}
			ps.close();
		}
		catch (SQLException e) {
			return "";
		}
		return result;
	}

	public boolean chgHDI(int cid, int year, float newHDI){
		int new_size;
		try {
			ps = connection.prepareStatement("UPDATE a2.hdi SET hdi_score = ? "
					+ "WHERE cid = ? AND year = ?");
			ps.setFloat(1, newHDI);
			ps.setInt(2, cid);
			ps.setInt(3, year);

			new_size = ps.executeUpdate();
			ps.close();
			if (new_size != 1) {
				return false;
			}
		} catch (SQLException e) {
			return false;
		}
		return true;
	}

	public boolean deleteNeighbour(int c1id, int c2id){
		int new_size1, new_size2;
		try {
			ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE (country = ?"
					+ " AND neighbor = ?)");
			ps.setInt(1, c1id);
			ps.setInt(2, c2id);

			new_size1 = ps.executeUpdate();
			ps.close();


			ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE (country = ?"
					+ " AND neighbor = ?)");
			ps.setInt(1, c2id);
			ps.setInt(2, c1id);

			new_size2 = ps.executeUpdate();
			ps.close();
		} catch (SQLException e) {
			return false;
		}
		if (new_size2 != 1 || new_size1 != 1) {
			return false;
		}
		return true;
	}

	public String listCountryLanguages(int cid){
		int totPop, lid;
		float lpercentage, langPop;
		String cname = "";
		String result = "";
		try {
			ps = connection.prepareStatement("SELECT cname, population FROM a2.country "
					+ " WHERE cid = ?");
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			if (rs.next()) {
				cname = rs.getString(1);
				totPop = rs.getInt(2);
			}
			else {
				return "";
			}
			ps.close();
			ps = connection.prepareStatement("SELECT lid, lpercentage"
					+ " FROM a2.language WHERE cid = ? ORDER BY lpercentage ASC");
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			while (rs.next()) {
				lid = rs.getInt(1);
				lpercentage = rs.getFloat(2);
				System.out.println(lpercentage);
				langPop = totPop * lpercentage;
				System.out.println(langPop);
				result += Integer.toString(lid) + ":"
						+ cname + ":"
						+ Float.toString(langPop) + "#";
			}
			result = result.substring(0, result.length() - 1);
			ps.close();
		} catch (SQLException e) {
			return "";
		}
		return result;
	}

	public boolean updateHeight(int cid, int decrH){
		int new_height, new_size;
		int old_height = 0;

		try {
			ps = connection.prepareStatement("SELECT height FROM a2.country "
					+ "WHERE cid = ?");
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			if (rs.next()) {
				old_height = rs.getInt(1);
			}
			new_height = old_height - decrH;
			ps = connection.prepareStatement("UPDATE a2.country SET height = ? "
					+ "WHERE cid = ?");
			ps.setInt(1, new_height);
			ps.setInt(2, cid);

			new_size = ps.executeUpdate();
			if (new_size != 1) {
				return false;
			}
		} catch (SQLException e) {
			return false;
		}
		return true;
	}

	public boolean updateDB(){
		try {
			ps = connection.prepareStatement("DROP TABLE IF EXISTS "
					+ "a2.mostPopulousCountries");
			ps.execute();
			ps.close();
			ps = connection.prepareStatement("CREATE TABLE "
					+ "a2.mostPopulousCountries (" +
					" cid INTEGER, cname VARCHAR(20))");
			ps.execute();
			ps.close();
			ps = connection.prepareStatement("INSERT INTO a2.mostPopulousCountries (SELECT cid, cname FROM a2.country WHERE population > 100000000 ORDER BY cid ASC)");
			ps.executeUpdate();
			ps.close();

		}  catch (SQLException e)  {
			return false;
		}
		return true;    
	}
}