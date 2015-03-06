import java.sql.*;

public class Assignment2
{
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
		catch (ClassNotFoundException e) 
		{
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
	}
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password)
	{
		try
		{
			connection = DriverManager.getConnection(URL, username, password);
			return true;
		} 
		catch (SQLException e)
		{
			return false;
		}
	}
  
	//Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB()
	{
		try
		{	//prevents memory leaks
			connection.close();
			ps.close();
			rs.close();
			//sql.close(); //didn't use it and it does hurt
			return true;
		} 
		catch (SQLException e)
		{
			return false;
		} 
	}
    
	public boolean insertCountry (int cid, String name, int height, int population) 
	{
		String template = "insert into a2.country values (?, ?, ?, ?);";
		try
		{
			ps = connection.prepareStatement(template);
			ps.setInt(1, cid);
			ps.setString(2, name);
			ps.setInt(3, height);
			ps.setInt(4, population);
			return ps.executeUpdate() == 1; //1 row effected
		} 
		catch (SQLException e)
		{
			// TODO Auto-generated catch block
			//e.printStackTrace();
			return false;
		}

	}
  
	public int getCountriesNextToOceanCount(int oid) 
	{
		String template = "select count(*) from a2.oceanaccess where oid=?;";
		try
		{
			ps = connection.prepareStatement(template);
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			rs.next();
			return rs.getInt(1);
			//the count(*) automatically handles when the oid doesn't exist
		} 
		catch (SQLException e)
		{
			// TODO Auto-generated catch block
			//e.printStackTrace();
			return -1;
		}
		
	}
   
	public String getOceanInfo(int oid)
	{
		String template = "select * from a2.ocean where oid=?;";
		try
		{
			ps = connection.prepareStatement(template);
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			if(rs.next())
			{
				return rs.getInt(1)+":"+rs.getString(2)+":"+rs.getInt(3);
			}
			else //if the ocean doesn't exist
			{
				return "";
			}
		}
		catch(SQLException e)
		{
			//e.printStackTrace();
			return "";
		}
	}

	public boolean chgHDI(int cid, int year, float newHDI)
  	{
		String template = "update a2.hdi set hdi_score=? where cid=? and year=?;";
		try
		{
			ps = connection.prepareStatement(template);
			ps.setFloat(1, newHDI);
			ps.setInt(2, cid);
			ps.setInt(3, year);
			return ps.executeUpdate() == 1; //only 1 row should be effected
		} 
		catch (SQLException e)
		{
			// TODO Auto-generated catch block
			//e.printStackTrace();
			return false;
		}
		
  	}

	public boolean deleteNeighbour(int c1id, int c2id)
	{
		String template = "delete from a2.neighbour where country=? and neighbor=?;";
		try
		{
			ps = connection.prepareStatement(template);
			ps.setInt(1, c1id);
			ps.setInt(2, c2id);
			//you don't know if it's stored as c1, c2 or c2, c1 in the db.
			//could be both
			
			//this commented out part below assumes no redundancy in the db.
			/**boolean firstTry = (ps.executeUpdate() == 1);
			if(firstTry)
			{
				return true;
			}
			else
			{
				ps.setInt(1, c2id);
				ps.setInt(2, c1id);
				return ps.executeUpdate() == 1;
				//if the border doesn't exist then executeUpdat would return 0
				//which is false. built in sanity checking
			}*/
			ps.executeUpdate();
			ps.setInt(1, c2id);
			ps.setInt(2, c1id);
			ps.executeUpdate();
			return true;
		} 
		catch (SQLException e)
		{
			// TODO Auto-generated catch block
			//e.printStackTrace();
			return false;
		}
	}
  
	public String listCountryLanguages(int cid)
	{
		String template = "select cid, lname, (lpercentage * population) as p from a2.country "
				+ "natural join a2.language where cid=? order by p;";
		try
		{
			String result = "";
			ps = connection.prepareStatement(template);
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			while(rs.next())
			{
				result = result + rs.getInt(1) + ":"
						+ rs.getString(2) + ":"
						+ rs.getFloat(3) + "#";
			}

			if(result.length() > 0)
			{
				//substring is 0 based
				//simply chop off the last #
				//the easiest laziest solution
				return result.substring(0, result.length()-1);
			}
			else
			{
				//forgot about the case where the country doesn't exist
				return result;
			}
		} 
		catch (SQLException e)
		{
			// TODO Auto-generated catch block
			//e.printStackTrace();
			return "";
		}
	}
  
	public boolean updateHeight(int cid, int decrH)
	{
		String get = "select height from a2.country where cid=?;";
		String template = "update a2.country set height=? where cid=?;";
		try
		{
			ps = connection.prepareStatement(get);
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			int old;
			if(!rs.next())
			{// check if country exists
				return false;
			}
			else
			{// check if new height is a valid decrease
				old = rs.getInt(1);
				if(decrH > old)
				{//can't decrease to a negative height
					return false;
				}
			}
			ps = connection.prepareStatement(template);
			int newheight = old - decrH;
			ps.setInt(1, newheight);
			ps.setInt(2, cid);
			return ps.executeUpdate() == 1; //only 1 row should be effected
		} 
		catch (SQLException e)
		{
			// TODO Auto-generated catch block
			//e.printStackTrace();
			return false;
		}	}
    
	public boolean updateDB()
	{
		String kill = "drop table if exists a2.mostPopulousCountries;";
		String fetch = "select cid, cname from a2.country "
				+ "where population>100000000 order by cid desc;";
		String mk = "create table a2.mostPopulousCountries "
				+ "(cid integer, "
				+ "cname varchar(20));";
		String insert = "insert into a2.mostPopulousCountries values (?, ?);";
		try
		{
			ps = connection.prepareStatement(kill);
			ps.executeUpdate();
			
			ps = connection.prepareStatement(mk);
			ps.executeUpdate();
			
			ps = connection.prepareStatement(fetch);
			rs = ps.executeQuery();
			
			while(rs.next())
			{
				ps = connection.prepareStatement(insert);
				ps.setInt(1, rs.getInt(1));
				ps.setString(2, rs.getString(2));
				ps.executeUpdate();
			}
			return true;
		} 
		catch (SQLException e)
		{
			// TODO Auto-generated catch block
			//e.printStackTrace();
			return false;
		}
		
	}
  
}
