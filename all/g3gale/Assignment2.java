import java.sql.*;

public class Assignment2
{
	// A connection to the database  
	private Connection connection = null;

	// Statement to run queries
	private Statement sql;

	// Prepared Statement
	private PreparedStatement ps;

	// Resultset for the query
	private ResultSet rs;

	// CONSTRUCTOR
	public Assignment2()
	{
		try 
		{
			Class.forName("org.postgresql.Driver");
		}

		catch (ClassNotFoundException e)
		{
			System.err.println("PostgreSQL JDBC Drive not found. Please check your library path!");
			e.printStackTrace();
		}
	}

	// Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password)
	{
		try
		{
			this.connection = DriverManager.getConnection(URL, username, password);
		}

		catch (SQLException e)
		{
			System.err.println("Connection failed!");
			e.printStackTrace();
			return false;
		}
		
		return this.connection != null;
	}

	// Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB()
	{
		try
		{
			this.connection.close();
			return true;
		}

		catch (SQLException e)
		{
			System.err.println("Error closing db!");
			e.printStackTrace();
		}

		return false;    
	}

	public boolean insertCountry (int cid, String name, int height, int population)
	{
		String queryString = "INSERT INTO a2.country VALUES (?, ?, ?, ?)";

		try
		{
			ps = connection.prepareStatement(queryString);

			ps.setInt(1, cid);
			ps.setString(2, name);
			ps.setInt(3, height);
			ps.setInt(4, population);

			ps.executeUpdate();
		}

		catch (SQLTimeoutException e)
		{
			System.err.println("Operation timed out.");
			e.printStackTrace();
			return false;
		}

		catch (SQLException e)
		{
			System.err.println("Couldn't Insert Values into country!");
			e.printStackTrace();
			return false;
		}

		return true;
	}

	public int getCountriesNextToOceanCount(int oid)
	{
		String queryString = "SELECT count(oa.cid) AS count FROM a2.oceanAccess oa WHERE oa.oid=?";
		int result;

		try
		{
			ps = connection.prepareStatement(queryString);

			ps.setInt(1, oid);

			rs = ps.executeQuery();

			result = rs.getInt(0);
		}

		catch (SQLTimeoutException e)
		{
			System.err.println("Operation timed out.");
			e.printStackTrace();
			return -1;
		}

		catch (SQLException e)
		{
			System.err.println("Couldn't Insert Values into country!");
			e.printStackTrace();
			return -1;
		}

		return result;
	}

	public String getOceanInfo(int oid)
	{
		String queryString = "SELECT * FROM a2.ocean WHERE ocean.oid=?";

		String name = "";
		int depth = 0;

		try
		{
			ps = connection.prepareStatement(queryString);

			ps.setInt(1, oid);

			rs = ps.executeQuery();

			name = rs.getString("oname");
			depth = rs.getInt("depth");
		}

		catch (SQLTimeoutException e)
		{
			System.err.println("Operation timed out.");
			e.printStackTrace();
			return "";
		}

		catch (SQLException e)
		{
			System.err.println("Couldn't Insert Values into country!");
			e.printStackTrace();
			return "";
		}

		String result = String.format("%d:%20s%d", oid, name, depth);

		return result;
	}

	public boolean chgHDI(int cid, int year, float newHDI)
	{
		String queryString = "UPDATE a2.hdi SET hdi_score=? WHERE hdi.cid=? AND hdi.year=?";

		try
		{
			ps = connection.prepareStatement(queryString);

			ps.setFloat(1, newHDI);
			ps.setInt(2, cid);
			ps.setInt(3, year);

			ps.executeUpdate();
		}

		catch (SQLTimeoutException e)
		{
			System.err.println("Operation timed out.");
			e.printStackTrace();
			return false;
		}

		catch (SQLException e)
		{
			System.err.println("Couldn't Insert Values into country!");
			e.printStackTrace();
			return false;
		}

		return true;
	}

	public boolean deleteNeighbour(int c1id, int c2id)
	{
		String queryString = "DELETE FROM a2.neighour n WHERE n.country=? AND n.neighbor=? OR n.country=? AND n.neighbor=?";

		try
		{
			ps = connection.prepareStatement(queryString);

			ps.setInt(1, c1id);
			ps.setInt(2, c2id);
			ps.setInt(3, c2id);
			ps.setInt(4, c1id);

			ps.executeUpdate();
		}

		catch (SQLTimeoutException e)
		{
			System.err.println("Operation timed out.");
			e.printStackTrace();
			return false;
		}

		catch (SQLException e)
		{
			System.err.println("Couldn't Insert Values into country!");
			e.printStackTrace();
			return false;
		}

		return true;      
	}

	public String listCountryLanguages(int cid)
	{
		String queryString = "SELECT l.lid AS lid, l.lname as lname, c.population*l.lpercentage AS population" +
								"FROM a2.language l JOIN a2.country c" +
								"ON l.cid=c.cid" +
								"WHERE language.cid=?";

		int lid = 0;
		String lname = "";
		float population = 0;

		try
		{
			ps = connection.prepareStatement(queryString);

			ps.setInt(1, cid);

			rs = ps.executeQuery();

			lid = rs.getInt("lid");
			lname = rs.getString("lname");
			population = rs.getFloat("population");
		}

		catch (SQLTimeoutException e)
		{
			System.err.println("Operation timed out.");
			e.printStackTrace();
			return "";
		}

		catch (SQLException e)
		{
			System.err.println("Couldn't Insert Values into country!");
			e.printStackTrace();
			return "";
		}

		String result = String.format("%d:%20s%d", lid, lname, population);

		return result;
	}

	public boolean updateHeight(int cid, int decrH)
	{
		String queryString = "UPDATE a2.country SET height=height-? WHERE country.cid=?";

		try
		{
			ps = connection.prepareStatement(queryString);

			ps.setInt(1, decrH);
			ps.setInt(2, cid);

			ps.executeUpdate();
		}

		catch (SQLTimeoutException e)
		{
			System.err.println("Operation timed out.");
			e.printStackTrace();
			return false;
		}

		catch (SQLException e)
		{
			System.err.println("Couldn't Insert Values into country!");
			e.printStackTrace();
			return false;
		}

		return true;
	}

	public boolean updateDB()
	{
		String queryString = "";
		return false;    
	} 

	public static void main(String[] args)
	{
		Assignment2 test = new Assignment2();
		test.connectDB("jdbc:postgresql://localhost:5432/csc343h-g3gale", "g3gale", "");

		/*
		test.insertCountry(11023123, "France", 20000, 1000000000);
		test.getCountriesNextToOceanCount(3);
		*/

		test.disconnectDB();
	}
}
