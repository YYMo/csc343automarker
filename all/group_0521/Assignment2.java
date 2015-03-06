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
      try 
		{
			connection = null;			
			Class.forName("org.postgresql.Driver");
		}
		catch (ClassNotFoundException e) 
		{
			System.exit(1);
		}
  }
  
  public boolean connectDB(String URL, String username, String password){
		try
		{
			connection = DriverManager.getConnection(URL, 
					username, password);
			
			if ( connection != null )
			{
				return true;
			}
		}
		catch(SQLException sqle)
		{
			System.out.println(sqle.getMessage());
		}
      return false;
  }
  
  public boolean disconnectDB(){
		try
		{
			connection.close();
			return true;
		}
		catch(SQLException sqle)
		{
			System.out.println(sqle.getMessage());
		}      
      return false;    
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
		try{
			sql = connection.createStatement();
			rs = null;
			String query = null;
			
			query = "select cid from a2.country where cid = " + cid;
			rs = sql.executeQuery(query);
			
			String CID;
			int rows;
                        
			if ( rs.next() )
			{
				CID = rs.getString(1);
				System.out.println("The a2.country already exists");
				return false;
			}
			else{
				query = "insert into a2.country values (" + cid + ",'" + name + "'," + height + "," +population + ")";
				rows = sql.executeUpdate(query);
				if (rows == 1)
				{
					sql.close();
					return true;
				}
				else
				{
					return false;
				}
			}
		}catch(SQLException sqle){
			System.err.println(sqle.getMessage());
		}
		      
   return false;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
      
      try
		{
			sql = connection.createStatement();
			rs = null;

			String query = "select count(*) from a2.oceanAccess where oid =" + oid;

			rs = sql.executeQuery(query);
		
		}
		catch(SQLException sqle)
		{
			System.err.println(sqle.getMessage());
		}
      
	return -1;  
  }
   
  public String getOceanInfo(int oid){
		rs = null;
		sql = null;
		String query = "";
                String info = "";
		try{
			sql = connection.createStatement();
			
			query = "select o.oid, o.oname, o.depth from a2.ocean o where o.oid=" + oid;
			
			rs = sql.executeQuery(query);
			
			if (rs.next())
			{
				info = info + rs.getString(1) + ":" + rs.getString(2) + ":" + rs.getString(3);
				return info;
			}
			else
			{
				return info;
			}
		}
		catch(SQLException sqle)
		{
			System.out.println(sqle.getMessage());
		}
		
		return info;
  }

  public boolean chgHDI(int cid, int year, float newHDI){
		try
		{
			int rows;	
			sql = connection.createStatement();
			String query;
			
			query = "UPDATE a2.hdi SET a2.hdi_score = " 
					+ newHDI +
					" where cid = " + cid + " and year = " + year; 
			rows = sql.executeUpdate(query);
			
			if ( rows == 1 )
			{
				sql.close();
				return true;
			}
		}
		catch(SQLException se)
		{
			System.out.println(se.getMessage());
		}      
   return false;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
		try
		{
			int rows;	
			sql = connection.createStatement();
			String query;
			
			query = "DELETE FROM a2.neighbour where c1id = " 
					+ c1id + " and c2id =" + c2id;
			rows = sql.executeUpdate(query);
			
			if (rows > 0 )
			{
				sql.close();
				return true;
			}
		}
		catch(SQLException sqle)
		{
			 System.out.println(sqle.getMessage());
		}
		      
   return false;        
  }
  
  public String listCountryLanguages(int cid){
		 
		rs = null;
		String query;
		sql = null;
		String cl = "";
                
		try{
			sql = connection.createStatement();
			
			query = "select l.lid, l.lname, c.population*l.lpercentage as population from a2.language l, a2.country c where l.cid = c.cid and l.cid=" + cid;

			rs = sql.executeQuery(query);
			int i;
			for (i = 0; rs.next(); i++)
			{
				if (i > 0)
					cl += "#";					
				cl += rs.getInt(1);
				cl += ":" + rs.getString(2); 
				cl += ":" + rs.getFloat(3);
                        }
			return cl;
		}
		catch(SQLException sqle)
		{
			System.out.println(sqle.getMessage());
		}
		     
	return "";
  }
  
  public boolean updateHeight(int cid, int decrH){
                try
		{
			int rows;	
			sql = connection.createStatement();
			String query;			
				
			query =  "UPDATE a2.country SET " +
					"height = height - " + decrH +
					" where cid = " + cid + "" ; 

			rows = sql.executeUpdate(query);
						
			if ( rows > 0 )
			{
				return true;
			}
		}
		catch(SQLException sqle)
		{
			System.err.println(sqle.getMessage());
		}
		      
    return false;
  }
    
  public boolean updateDB(){
	{
		try
		{
			sql = null;
			rs = null;
			int rows = 0;
			String query1  = "";
			String query2 = "";
			
			sql = connection.createStatement();
			
			query1 = "create table mostPopulousCountries(cid INTEGER, cname VARCHAR(20))";
			sql.executeUpdate(query1);
			
			query2 = "insert into mostPopulousCountries (select c.cid, c.cname from a2.country c where c.population > 100000000 ORDER BY c.cid ASC)";
			sql.executeUpdate(query2);

			rows = sql.executeUpdate(query2);
			if (rows > 0)
			{
				sql.close();
				return true;
			}
			else
			{
				return false;
			}
		}
		catch(SQLException sqle)
	   {
			System.out.println(sqle.getMessage());
	   }      
	return false;    
  }
  
  }
  
}