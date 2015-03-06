<!DOCTYPE html>
<html>
<body>
  <pre>import java.sql.*;

public class Assignment2 {

	// A connection to the database
	Connection connection;

	// Statement to run queries
	Statement sql;

	// Prepared Statement
	PreparedStatement ps;

	// Resultset for the query
	ResultSet rs;

	// CONSTRUCTOR
	Assignment2() {
		try {
			// Load JDBC driver
			Class.forName(&quot;org.postgresql.jdbc.Driver&quot;);
		} catch (ClassNotFoundException e) {
			//return;
		}
	}

	// Using the input parameters, establish a connection to be used for this
	// session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password) {
		try {
			connection = DriverManager.getConnection(URL, username, password);
			return true;
		} catch (SQLException e) {
			return false;
		}
	}

	// Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB() {
		try {
			connection.close();
			return true;
		} catch (SQLException e) {
			return false;
		}

	}

	public boolean insertCountry(int cid, String name, int height, int population) {
		try {  
			
			ps = connection.prepareStatement(&quot;INSERT INTO a2.country VALUES( ?, ?, ?, ?);&quot;);
			ps.setInt(1, cid);
			ps.setString(2, name);
			ps.setInt(3, height);
			ps.setInt(4, population);
			if (ps.executeUpdate() == 0) {
				return false;
			}
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	public int getCountriesNextToOceanCount(int oid) {
		try {
			ps = connection.prepareStatement(&quot;Select COUNT(cid) AS countryCount FROM a2.oceanAccess WHERE oid = ? ;&quot;);
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			if(rs.next()){
				return rs.getInt(&quot;countryCount&quot;);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		}
	}

	public String getOceanInfo(int oid) {
		try {
			String result = &quot;&quot;;

			// Prepare Statement
			ps = connection.prepareStatement(&quot;SELECT * FROM a2.ocean WHERE oid = ?;&quot;);

			ps.setInt(1, oid);

			// Execute Statement and obtain ResultSet
			rs = ps.executeQuery();

			// Process the Results
			if(rs.next()){result = rs.getString(&quot;oid&quot;) + &quot;:&quot; + rs.getString(&quot;oname&quot;) + &quot;:&quot;
					+ rs.getString(&quot;depth&quot;);
			}
			if (result.isEmpty()) {
				return &quot;&quot;;
			}
			return result;

		} catch (SQLException e) {
			e.printStackTrace();
			return &quot;&quot;;
		}
	}

	public boolean chgHDI(int cid, int year, float newHDI) {
		try {
			ps = connection.prepareStatement(&quot;UPDATE hdi SET a2.hdi_score = ? WHERE cid = ? AND year = ?;&quot;);
			ps.setFloat(1, newHDI);
			ps.setInt(2, cid);
			ps.setInt(3, year);

			if (ps.executeUpdate() == 0) {
				return false;
			}
			return true;

		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean deleteNeighbour(int c1id, int c2id) {
		try {
			ps = connection.prepareStatement(&quot;DELETE FROM a2.neighbour WHERE (country = ? AND neighbour= ?) OR (country = ? AND neighbour= ?);&quot;);
			ps.setInt(1, c1id);
			ps.setInt(2, c2id);
			ps.setInt(3, c2id);
			ps.setInt(4, c1id);
			if (ps.executeUpdate() == 0) {
				return false;
			} else {
				return true;
			}

		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}

	}

	public String listCountryLanguages(int cid) {
		try {
			String result = &quot;&quot;;

			// Prepare Statement
			ps = connection
					.prepareStatement(&quot;SELECT lid, lname, (population*percentage) as population&quot;
							+ &quot;FROM a2.country, a2.language &quot;
							+ &quot;WHERE country.cid = language.cid AND country.cid = ?&quot;
							+ &quot;ORDER BY population;&quot;);

			ps.setInt(1, cid);

			// Execute Statement and obtain ResultSet
			rs = ps.executeQuery();

			// Process the Results
			while (rs.next()) {
				result = result + &quot;|&quot; + rs.getInt(&quot;lid&quot;) + &quot;:&quot; + rs.getString(&quot;lname&quot;)
						+ &quot;:&quot; + rs.getInt(&quot;population&quot;) + &quot;#&quot;;
			}
			if (result.length() != 0) {
				result = result.substring(0, result.length()-1);
			}
			return result;

		} catch (SQLException e) {
			e.printStackTrace();
			return &quot;&quot;;
		}
	}

	public boolean updateHeight(int cid, int decrH) {
		try {
			ps = connection.prepareStatement(&quot;UPDATE a2.country SET height = height - ?  WHERE cid = ? ;&quot;);
			ps.setInt(1, decrH);
			ps.setInt(2, cid);

			if (ps.executeUpdate() == 0) {
				return false;
			}
			return true;

		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean updateDB() {
		try {
			ps = connection.prepareStatement(&quot;CREATE TABLE a2.mostPopulousCountries (&quot;
							+ &quot; cid INTEGER, &quot; + &quot; cname VARCHAR(20) );&quot;);

			ps.executeUpdate();

			ps = connection.prepareStatement(&quot;INSERT INTO a2.mostPopulousCountries (SELECT cid, cname FROM a2.country WHERE population &gt; 100000000 &quot;
							+ &quot;ORDER BY cid);&quot;);

			if (ps.executeUpdate() == 0) {
				return false;
			}
			return true;

		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

}
</pre>
</body>
</html>
