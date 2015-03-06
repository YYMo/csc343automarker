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
	try {
		Class.forName("org.postgresql.Driver");
	} catch (ClassNotFoundException e) {
		e.printStackTrace();
	}
	
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	try{
		connection = DriverManager.getConnection(URL, username, password);
	} catch (SQLException e) {
		return false;
	}
	if (connection != null) {
		return true;
	} else{
      return false;
	}
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
	try{
		connection.close();
	} catch (SQLException e) {
		return false;
	}
	return true;
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
	
	boolean ok = true;
	try{
		sql = connection.createStatement();
		String sqlText;
		sqlText = "SELECT c.cid " +
			"FROM country c " +
			"WHERE c.cid = " + cid;
		rs = sql.executeQuery(sqlText);
		String temp; temp = "";
		if (rs != null){
			if(rs.next()){
				temp = rs.getString(1);
			}
		}
		rs.close();
		if (temp == ""){
			ok = true;
			sqlText = "INSERT INTO country " +
			"VALUES (?,?,?,?) ";
			ps = connection.prepareStatement(sqlText);
			ps.setInt(1,cid);
			ps.setString(2,name);
			ps.setInt(3,height);
			ps.setInt(4,population);
			ps.executeUpdate();
			ps.close();
		}else{ok = false;}
	} catch (SQLException e){
	return false;
	}
	return ok;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	int temp = 0;
	try{
		sql = connection.createStatement();
		String sqlText;

		sqlText = "SELECT count(cid) " +
		"FROM oceanAccess " + 
		"WHERE oid = " + oid;
		
		rs = sql.executeQuery(sqlText);
		if (rs != null){
			if(rs.next()){
				temp = rs.getInt(1);
			}
		}
		rs.close();
		
	} catch (SQLException e){
		return -1; 
	}
	return temp;
	
  }
   
  public String getOceanInfo(int oid){
	String info = "";
	try{
		sql = connection.createStatement();
		String sqlText;
		sqlText = "SELECT oid " +
			"FROM ocean o " +
			"WHERE o.oid = " + oid;
			
		rs = sql.executeQuery(sqlText);
		String temp; temp = "";
		if (rs != null){
			temp = rs.getString(1);
		}
		if (temp != ""){
			sqlText = "SELECT oid, oname, depth " +
			"FROM ocean o " +
			"WHERE o.oid = " + oid;
			rs = sql.executeQuery(sqlText);
			if (rs != null){
				if(rs.next()){
					temp = "" + rs.getInt(1);
					info = temp + ":";
					temp = rs.getString(2);
					info = info + temp + ":";
					temp = "" + rs.getInt(3);
					info = info + temp;
				}
			}
		}
		rs.close();
	} catch (SQLException e){
		return "";
	}
	return info;
  }

  public boolean chgHDI(int cid, int year, float newHDI){

	try{
		sql = connection.createStatement();
		String sqlText;
		sqlText = "UPDATE hdi " +
			"SET hdi_score = " + newHDI +
			" WHERE cid = " + cid +
			" and year = " + year;
		sql.executeUpdate(sqlText);
	} catch (SQLException e){
		return false;
	}
	return true;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	try{
		sql = connection.createStatement();
		String sqlText;
		sqlText = "DELETE FROM neighbour " +
			"WHERE (country = " + c1id +
			" and neighbor = " + c2id +
			") or (country = " + c2id +
			" and neighbor = " + c1id + ")";
		sql.executeUpdate(sqlText);
	} catch (SQLException e){
		return false;
	}
	return true;       
  }
  
  public String listCountryLanguages(int cid){
	String info = "";
	try{
		sql = connection.createStatement();
		String sqlText;
		sqlText = "SELECT lid, lname, (lpercentage*population*0.01) as lper " +
			"FROM language l, country c " +
			"WHERE c.cid = l.cid = " + cid +
			" order by lper";
		rs = sql.executeQuery(sqlText);	
		if (rs != null){
			if(rs.next()){
			info = "" + rs.getInt(1) + ":";
			info = info + rs.getString(2) + ":";
			info = info + rs.getInt(3);
			}
			while (rs.next()){
				info = info + "#" + rs.getInt(1) + ":";
				info = info + rs.getString(2) + ":";
				info = info + rs.getInt(3);
			}
		}
		rs.close();
	} catch (SQLException e){
		return "";
	}
	return info;	

  }
  
  public boolean updateHeight(int cid, int decrH){
	int temp = 0;
	try{
		sql = connection.createStatement();
		String sqlText;
		sqlText = "SELECT height " +
		"FROM country " + 
		"WHERE cid = " + cid;
		
		rs = sql.executeQuery(sqlText);		
		if (rs != null){
			if(rs.next()){
				temp = rs.getInt(1);
			}
		}
		temp -= decrH;
		rs.close();
		
		sqlText = "UPDATE country " +
		"SET height = " + temp +
		" WHERE cid = " + cid;
		sql.executeUpdate(sqlText);
	} catch (SQLException e){
		return false; 
	}
    return true;
  }
    
  public boolean updateDB(){
	int a = 0;
	String b = "";
	try{
		sql = connection.createStatement();
		String sqlText;
		sqlText = "create table mostPopulousCountries( " +
		"cid INTEGER, " + 
		"cname VARCHAR(20) ";
		sql.executeUpdate(sqlText);
		
		sqlText = "SELECT cid, cname " +
		"FROM country " + 
		"WHERE population > 100000000 " +
		"order by cid";
		rs = sql.executeQuery(sqlText);		
		if (rs != null){
			sqlText = "INSERT INTO mostPopulousCountries " +
			"VALUES (?,?) ";
			ps = connection.prepareStatement(sqlText);
			while (rs.next()){
				a = rs.getInt(1);
				b = rs.getString(2);		
				ps.setInt(1,a);
				ps.setString(2,b);
				ps.executeUpdate();			
			}
			ps.close();
		}
		rs.close();
	} catch (SQLException e){
		return false; 
	}
    return true;   
  }
  
}
