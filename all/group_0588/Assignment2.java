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
	try{
		Class.forName("org.postgresql.Driver");
	}catch (Exception e){
	}
  }
  
  public void closeAllConnections(PreparedStatement ps, ResultSet rs){
       try{
		if (ps != null){
			ps.close();
		}		
		if (rs != null){
			rs.close();
		}
	}catch (SQLException ex){

	}
  }

  //Using the input parameters, establish a connection to be used for this session. 
  //Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
  
		try{
			connection = DriverManager.getConnection(URL, username, password);
			return true;
		}catch(Exception e){
			return false;
		}    
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      
		try{
			connection.close();
			if(connection.isClosed()){
				return true;
			}
			return false;
		}catch (SQLException e){
			return false;
		}    
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
		try{
			String query = "select cid from country where cid=?";
			ps = connection.prepareStatement(query);
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			ps.close();
			if(!rs.next()){
				String updateQuery = "insert into country(cid, name, height, population) values (?, ?, ?, ?)";
				ps = connection.prepareStatement(updateQuery);
				ps.setInt(1, cid);
				ps.setString(2, name);
				ps.setInt(3, height);
				ps.setInt(4, population);
				ps.executeUpdate();
				ps.close();
				rs.close();
				return true;
			} 
			closeAllConnections(ps, rs);
			return false;
		}catch (SQLException ex){
			closeAllConnections(ps, rs);
			return false;
		}
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	try{
		String query = "select count(distinct cid) from oceanAccess where oid=?";
		ps = connection.prepareStatement(query);
		ps.setInt(1, oid);
		ps.executeQuery();
		ps.close();
		return 1;
	}catch (SQLException ex){
		closeAllConnections(ps, null);
		return -1;
	}
  }
   
  public String getOceanInfo(int oid){
   	try{
		String query = "select oid, oname, depth from ocean where oid=?";
		String getResults = "";
		ps = connection.prepareStatement(query);
		ps.setInt(1, oid);
		rs = ps.executeQuery();
		
		String oidColumn = rs.getString(1);
		String onameColumn = rs.getString(2);
		String depthColumn = rs.getString(3);
		
		String finalString = oidColumn + ":" + onameColumn + ":" + depthColumn;

		rs.close();
		ps.close();
		return finalString;
	}catch (SQLException ex){
		closeAllConnections(ps, rs);
		return "";
	}
  }
  public boolean chgHDI(int cid, int year, float newHDI){
   
	try{
		String query1 ="select hid from hdi where cid=? AND year=?";
		ps = connection.prepareStatement(query1);
		ps.setInt(1, cid);
		ps.setInt(2, year);
		rs = ps.executeQuery();
		ps.close();

		if(rs.next()){		
			String query = "update hdi set hdi_score=? where cid=? AND year=?";
			ps = connection.prepareStatement(query);
			ps.setFloat(1, newHDI);
			ps.setInt(2, cid);
			ps.setInt(3, year);
			ps.executeUpdate();
			ps.close();
			rs.close();
			return true;
		}
		closeAllConnections(ps, rs);
		return false;
	}catch(SQLException e){
		closeAllConnections(ps, rs);
		return false;
	}

  }
  public boolean deleteNeighbour(int c1id, int c2id){
   	try{
		String query = "select * from neighbour where country = ? and neighbor = ?";
		String query1 = "select * from neighbour where country = ? and neighbor = ?";
		String finalQuery = query + " UNION "+ query1;
		ps = connection.prepareStatement(finalQuery);
                ps.setInt(1, c1id);
		ps.setInt(2, c2id);
		ps.setInt(3, c2id);
		ps.setInt(4, c1id);
                rs = ps.executeQuery();
		if (rs.rowDeleted()){
			ps.close();
			rs.close();
			return true;
		}
		closeAllConnections(ps, rs);
		return false;
	}catch (SQLException ex){
		closeAllConnections(ps, rs);
		return false;
	}
  }
  
  public String listCountryLanguages(int cid){
	
	try{
		String query = "select lid, lname, (population * lpercentage) from language JOIN country where cid = ?";
		String lid;
		String lname;
		String pop;
		String answer = "";
		ps = connection.prepareStatement(query);
		ps.setInt(1, cid);
		rs = ps.executeQuery();

		while(rs.next()){
			lid = rs.getString(1);
			lname = rs.getString(2);
			pop = rs.getString(3);
			answer = answer + "#" + lid + ":" + lname + ":" + pop;
		}
		closeAllConnections(ps, rs);
		return answer;
	}catch(SQLException e){
		closeAllConnections(ps, rs);
		return "";
	}
  }
  
  public boolean updateHeight(int cid, int decrH){
	try{
		String query = "update country set height = (height - ?) where cid = ?";
		String query2 = "select cid from country where cid = ?";
		ps = connection.prepareStatement(query2);
		ps.setInt(1, cid);
		rs = ps.executeQuery(query2);
		ps.close();
		if(rs.next()){
			ps = connection.prepareStatement(query);
			ps.setInt(1, decrH);
			ps.setInt(2, cid);
			ps.executeUpdate();
			ps.close();
			rs.close();
			return true;
		}
		closeAllConnections(ps, rs);
		return false;
	}catch(SQLException e){
		closeAllConnections(ps, rs);
		return false;
	}
  }
    
  public boolean updateDB(){
	// alex is awesome
	try{
		String query = "CREATE TABLE IF NOT EXISTS mostPopulousCountries (cid INTEGER REFERENCES country(cid), cname VARCHAR(20) NOT NULL, PRIMARY KEY(cid, cname)";
		ps = connection.prepareStatement(query);
		ps.executeUpdate();
		ps.close();

		query = "insert into mostPopulousCountries ";
		String subquery = "(select cid, cname from country where population > 100000000)";
		ps = connection.prepareStatement(subquery);
		rs = ps.executeQuery();
		ps.close();

		if(rs.next()){
			ps = connection.prepareStatement(query + subquery);
			ps.executeUpdate();
			ps.close();
			rs.close();
			return true;
		}
		closeAllConnections(ps, rs);
		return false;
	}catch(SQLException e){
		closeAllConnections(ps, rs);
		return false;
	}
  }		
  
}
