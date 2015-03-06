import java.sql.*;

public class Assignment2 {
    
  // A connection to the database  
  Connection conn;
  
  // Statement to run queries
  Statement sql;
  
  // Prepared Statement
  PreparedStatement ps;
  
  // Resultset for the query
  ResultSet rs;
  
  //CONSTRUCTOR
  Assignment2(){
	try{
	Class.forName("org.postgresql.jdbc.Driver");
	}catch(ClassNotFoundException se){
		return;		
	}
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      	try{
		conn = DriverManager.getConnection(URL, username, password);
		ps = conn.prepareStatement("SET search_path TO A2 ");
		ps.executeUpdate();
		return true;
	}      
	catch(SQLException se){
		return false;	
	}
}
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try{conn.close();
	return true;

	}
	catch(SQLException se){
		return false;	
	}
	  
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
   	try{
	sql = conn.createStatement();
	ps = conn.prepareStatement("INSERT INTO country(cid, cname, height, population) " +
			"VALUES (?, ?, ?, ?)");
	ps.setInt(1, cid);
	ps.setString(2, name);
	ps.setInt(3, height);
	ps.setInt(4, population);
	ps.executeUpdate();
		return true;
	}catch(SQLException se){
		return false;}
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	try{
		String qu = "SELECT COUNT(oid) AS noc " + "FROM 					oceanAccess " + "WHERE oid = " 						+ oid + "GROUP BY oid";
		sql = conn.createStatement();
		rs = sql.executeQuery(qu);
		if (rs.next()){
			return rs.getInt("noc");
		}else {return -1;}
	}catch(SQLException se){
		return -1;}	
  }
   
  public String getOceanInfo(int oid){
	try{
		String qu = "SELECT * " + "FROM ocean " + "WHERE oid = " + oid ;
		sql = conn.createStatement();
		rs = sql.executeQuery(qu);
		if(rs.next()){
			String result = oid+":"+rs.getString("oname")+":"+rs.getInt("depth");
			return result;
		}else{ 
			return ""; 
		}
	} catch(SQLException se){
		return "";
	}
  }

  public boolean chgHDI(int cid, int year, float newHDI){
 	try{
		sql = conn.createStatement();
		String cH = "UPDATE hdi " + "SET hdi_score = " + newHDI + "WHERE cid = " + cid + "AND year = " + year;
		sql.executeUpdate(cH);
		return true;
	}catch(SQLException se){
		
		return false;
	}	
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	try{	
		sql = conn.createStatement();
		String dN = "DELETE FROM neighbour " + "WHERE country = " + c1id + "AND neighbor = " + c2id + "AND country = " + c2id + "AND neighbor = " + c1id ; 
		sql.executeUpdate(dN);  
		return true;	
	}catch(SQLException se){   

		return false;       
	}
  }
  
  public String listCountryLanguages(int cid){
	try{	sql = conn.createStatement();
		String lcl = "SELECT * , population*lpercentage AS f " + "FROM (country NATURAL JOIN language) " + "WHERE cid = " + cid + "ORDER BY f "; 
		rs=sql.executeQuery(lcl);
		String xs = new String("");		
		while(rs.next()){
			xs = xs + rs.getInt("lid")+":"+rs.getString("lname")+":"+rs.getInt("f")+"#";
		}
		return xs.substring(0,xs.length()-1);
	}catch(SQLException se){	
		return "";
	}
  }
  
  public boolean updateHeight(int cid, int decrH){
	try{
		sql = conn.createStatement();
		String gH = "SELECT * " + "FROM country " + "WHERE cid = " + cid; 
		rs = sql.executeQuery(gH);
		if(rs.next()){
		String uH = "UPDATE country " + "SET height = " + (rs.getInt("height")-decrH) + "WHERE cid = " + cid;
		sql.executeUpdate(uH);
		}
		return true;	
	}catch(SQLException se){ 

		return false;
	}
  }
    
  public boolean updateDB(){
	try{
		sql = conn.createStatement();
		String gtb = "CREATE TABLE mostPopulousCountries " + "(cid INTEGER, " + "cname VARCHAR(20), " + "PRIMARY KEY (cid))";
		sql.executeUpdate(gtb);
		String quer = "SELECT cid, cname " + "FROM country " + "WHERE population > 100000000 " + "ORDER BY cid ASC ";
		rs = sql.executeQuery(quer);
		while(rs.next()){
			ps = conn.prepareStatement("INSERT INTO mostPopulousCountries(cid, cname) VALUES(?,?)");
			ps.setInt(1, rs.getInt("cid"));
			ps.setString(2, rs.getString("cname"));
			ps.executeUpdate();		
		}
		return true;
	}catch(SQLException se){
		return false;    
	}
  }
  
}
