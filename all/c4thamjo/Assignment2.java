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
        }
        catch(ClassNotFoundException ce){
            return;
        }
    }
	  
    //Using the input parameters, establish a connection to be used for this session. Returns true if connection is successful
    public boolean connectDB(String URL, String username, String password){		
		try{
			connection = DriverManager.getConnection(URL,username,password);
			return connection.isValid(0);
		}
		catch(SQLException se){
			return false;
		}
    }
	
	//Closes the connection. Returns true if closure was successful
	public boolean disconnectDB(){
		try {
			connection.close();
			return connection.isClosed();
		}
		catch (Exception e) {
			return false;
		}
	}
    
	public boolean insertCountry (int cid, String name, int height, int population) {
		try{
			ps = connection.prepareStatement("select * from a2.country where cid = ?");
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			if (!rs.isBeforeFirst()){
				ps = connection.prepareStatement("insert into a2.country(cid, cname, height, population) values (?,?,?,?)");
				ps.setInt(1,cid);
				ps.setString(2,name);
				ps.setInt(3,height);
				ps.setInt(4,population);}
				if(ps.executeUpdate() == 1){
					closeAll();
					return true;
				}
			}
		catch(SQLException e){
			closeAll();
			return false;
		}
		closeAll();
		return false;
	}
  
	public int getCountriesNextToOceanCount(int oid) {
		int retval;
		try{
			ps = connection.prepareStatement("select count(*) from a2.oceanAccess where oid = ?");
			ps.setInt(1,oid);
			rs = ps.executeQuery();
			rs.next();
			retval = rs.getInt(1);
			closeAll();
		}
		catch(SQLException se){
			retval = -1;
			closeAll();
		}
		return retval;
	}
   
	public String getOceanInfo(int oid){
		String retval = "";
		try{
			ps = connection.prepareStatement("select * from a2.ocean where oid = ?");
			ps.setInt(1,oid);
			rs = ps.executeQuery();
			rs.next();
			StringBuilder sb = new StringBuilder();
			sb.append(rs.getString(1).trim());
			sb.append(':');
			sb.append(rs.getString(2).trim());
			sb.append(':');
			sb.append(rs.getString(3).trim());
			retval = sb.toString();
			closeAll();
		}
		catch(SQLException se){
			closeAll();
			retval = "";
		}
		return retval;
	}

	public boolean chgHDI(int cid, int year, float newHDI){
		try{
			ps = connection.prepareStatement("update a2.hdi set hdi_score = ? where cid = ? and year = ?");
			ps.setFloat(1,newHDI);
			ps.setInt(2,cid);
			ps.setInt(3,year);
			if (ps.executeUpdate() == 1){
				closeAll();
				return true;
			}
		}
		catch(SQLException se){
			closeAll();
			return false;
		}
		closeAll();
		return false;
	}

	public boolean deleteNeighbour(int c1id, int c2id){
		try{
			ps = connection.prepareStatement("delete from a2.neighbour where country = ? and neighbor = ?");
			ps.setInt(1,c1id);
			ps.setInt(2,c2id);
			ps.executeUpdate();
			closeAll();
			
			ps = connection.prepareStatement("delete from a2.neighbour where country = ? and neighbor = ?");
			ps.setInt(1,c2id);
			ps.setInt(2,c1id);
			ps.executeUpdate();
			closeAll();
			return true;
		}
		catch(SQLException se){
			closeAll();
			return false;
		}
	}
  
	public String listCountryLanguages(int cid){
		String retval = "";
		try{
			ps = connection.prepareStatement("select lid, lname, lpercentage*population as number from a2.language, a2.country where a2.language.cid = ? and a2.country.cid = language.cid order by number");
			ps.setInt(1,cid);
			rs = ps.executeQuery();
			StringBuilder sb = new StringBuilder();
			while(rs.next()){
				sb.append(rs.getString(1).trim());
				sb.append(':');
				sb.append(rs.getString(2).trim());
				sb.append(':');
				sb.append(rs.getString(3).trim());
				if (!rs.isLast())
					sb.append('#');
			}
			retval = sb.toString();
		}
		catch(SQLException se){
			retval = "";
		}
		closeAll();
		return retval;
	}
  
	public boolean updateHeight(int cid, int decrH){
		try{
			ps = connection.prepareStatement("select height from a2.country where cid = ?");
			ps.setInt(1,cid);
			rs = ps.executeQuery();
			if (rs.next()){
				int oldH = rs.getInt(1);
				closeAll();
				ps = connection.prepareStatement("update a2.country set height = ? where cid = ?");
				ps.setInt(1,oldH - decrH);
				ps.setInt(2,cid);
				if (ps.executeUpdate() >= 0){
					closeAll();
					return true;
				}
			}
		}
		catch(SQLException se){
			closeAll();
			return false;
		}
		closeAll();
		return false;
	}
    
	public boolean updateDB(){
		try {
			sql	= connection.createStatement();
			sql.execute("create table a2.mostPopulousCountries (cid INTEGER, cname VARCHAR(20))");			
			if (sql.executeUpdate("insert into a2.mostPopulousCountries (select cid, cname from a2.country where population > 100000000 order by cid asc)") >= 0){
				closeAll();
				return true;
				}
			closeAll();
			return false;
			
		}
		catch(SQLException se){
			closeAll();
			return false;
		}
	}
	
	public void closeAll(){
		try {
			ps.close();
			rs.close();
			sql.close();
		} catch (Exception e) {
		}
	} 
}
