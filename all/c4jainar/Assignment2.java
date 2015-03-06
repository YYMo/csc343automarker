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
	Assignment2()
	{
		try
		{
			Class.forName("org.postgresql.Driver");
		}
		catch(ClassNotFoundException e) 
		{
			System.exit(1);
		}
	}
	
	//Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password)
	{
		try
		{
			connection = DriverManager.getConnection(URL, username, password);
			return connection != null;
		}
		catch(SQLException se)
		{
			return false;
		}
	}
	
	//Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB()
	{
		try
		{
			connection.close();
			return true;
		}
		catch(SQLException se)
		{
			return false;
		}
	}
	
	public boolean insertCountry (int cid, String name, int height, int population) 
	{
		try
		{
			ps = connection.prepareStatement(
				"SELECT * FROM a2.country WHERE cid=?"
				);
			ps.setInt(1, cid);
	
			rs = ps.executeQuery();

			if(rs.next())
			{
				ps.close();
				return false;	
			}

			ps.close();

			ps = connection.prepareStatement(
				"INSERT INTO a2.country VALUES(?, ?, ?, ?)"
				);
			ps.setInt(1, cid);
			ps.setString(2, name);
			ps.setInt(3, height);
			ps.setInt(4, population);
	
			ps.executeUpdate();
			
			ps.close();
			return true;
		}
		catch(SQLException se)
		{
			return false;
		}

	}
	 
	public int getCountriesNextToOceanCount(int oid) 
	{
		try
		{
			int count = 0;
			ps = connection.prepareStatement(
				"SELECT * FROM a2.oceanAccess WHERE oid=?"
				);
			ps.setInt(1, oid);
			
			rs = ps.executeQuery();	
			
			while(rs.next())
			{
				count++;
			}

			ps.close();
			return count;
		}
		catch(SQLException se)
		{
			return -1;			
		}
	}

	public String getOceanInfo(int oid)
	{
		try
		{
			ps = connection.prepareStatement(
				"SELECT * FROM a2.ocean WHERE oid=?"
				);
			ps.setInt(1, oid);

			rs = ps.executeQuery();

			if(!rs.next())
			{
				ps.close();
				return "";
			}

			String result = rs.getInt(1) + ":" + rs.getString(2) + ":" + rs.getInt(3);

			ps.close();
			return result;
		}
		catch(SQLException se)
		{
			return "";			
		}
	}

	public boolean chgHDI(int cid, int year, float newHDI)
	{
	 	try
		{
			ps = connection.prepareStatement(
				"SELECT * FROM a2.hdi WHERE cid=? AND year=?"
				);
			ps.setInt(1, cid);
			ps.setInt(2, year);
	
			rs = ps.executeQuery();

			if(!rs.next())
			{
				ps.close();
				return false;	
			}

			ps.close();

			ps = connection.prepareStatement(
				"UPDATE a2.hdi SET hdi_score=? WHERE cid=? AND year=?"
				);
			ps.setFloat(1, newHDI);
			ps.setInt(2, cid);
			ps.setInt(3, year);
	
			ps.executeUpdate();
			
			ps.close();
			return true;
		}
		catch(SQLException se)
		{
			return false;
		}	 
	}

	public boolean deleteNeighbour(int c1id, int c2id)
	{
		try
		{
			ps = connection.prepareStatement(
				"DELETE FROM a2.neighbour WHERE (country=? AND neighbor=?) OR (country=? AND neighbor=?)"
				);
			ps.setInt(1, c1id);
			ps.setInt(2, c2id);
			ps.setInt(3, c2id);
			ps.setInt(4, c1id);
		
			ps.executeUpdate();
			
			ps.close();
			return true;
		}
		catch(SQLException se)
		{
			return false;
		}	 		
	}

	public String listCountryLanguages(int cid)
	{
		try
		{
			ps = connection.prepareStatement(
				"SELECT lid, lname, (lpercentage*population) AS lpopulation   " +
				 "FROM a2.language L join a2.country C ON L.cid = C.cid    " +
				 "WHERE L.cid=?    " +
				 "ORDER BY (lpercentage*population)"
				);
			ps.setInt(1, cid);
			
			rs = ps.executeQuery();	
			
			StringBuffer result = new StringBuffer();
			int id = 1;
			
			while(rs.next())
			{
				result.append("l" + id + rs.getInt(1) + ":l" + id + rs.getString(2) + ":l" + id + rs.getFloat(3) + "#");
				id++;
			}

			ps.close();
			return result.toString();
		}
		catch(SQLException se)
		{
			return "";			
		}
	}

	public boolean updateHeight(int cid, int decrH)
	{
	 	try
		{
			ps = connection.prepareStatement(
				"SELECT * FROM a2.country WHERE cid=?"
				);
			ps.setInt(1, cid);
	
			rs = ps.executeQuery();

			if(!rs.next())
			{
				ps.close();
				return false;	
			}

			int newH = rs.getInt("height") - decrH;

			ps.close();

			ps = connection.prepareStatement(
				"UPDATE a2.country SET height=? WHERE cid=?"
				);
			ps.setInt(1, newH);
			ps.setInt(2, cid);
	
			ps.executeUpdate();
			
			ps.close();
			return true;
		}
		catch(SQLException se)
		{
			return false;
		}	 
	}

	public boolean updateDB()
	{
		try
		{
			ps = connection.prepareStatement(
				"CREATE TABLE a2.mostPopulousCountries (    " +
			    "cid 		INTEGER 	PRIMARY KEY,    " +
			    "cname 		VARCHAR(20)	NOT NULL)"
				);			
			
			ps.executeUpdate();	

			ps.close();

			ps = connection.prepareStatement(
				"INSERT INTO a2.mostPopulousCountries    " +
				 "(SELECT cid, cname FROM a2.country WHERE population > 100000000 ORDER BY cid)"
				);
			
			ps.executeUpdate();	

			ps.close();
			return true;
		}
		catch(SQLException se)
		{
			return false;			
		}
	}

}
