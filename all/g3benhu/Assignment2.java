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
  
  int count;
  
  String s;
  String temp;
  
  //CONSTRUCTOR
  Assignment2(){
	try {
		Class.forName("org.postgresql.Driver");
	} catch (Exception e) {
		e.printStackTrace();
	}
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	  try {
		  connection = DriverManager.getConnection(URL, username, password);
	  } catch (SQLException se) {
		  se.printStackTrace();
		  return false;
	  } catch (Exception e) {
		  e.printStackTrace(); 
		  return false;
	  }
	  return true;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
	try{
		if(sql!=null){
			sql.close();
		  } 
	}catch(SQLException se){
			  se.printStackTrace();
			  return false;
	} 
	
	try{
		if(ps!=null){
			ps.close();
		}
	} catch(SQLException se){
			se.printStackTrace();  
			return false;
	}
	try{
		if(rs!=null){
			rs.close();
		} 
	}catch(SQLException se){
			se.printStackTrace(); 
			return false;
	}
	
	try{
		if(connection!=null){
			connection.close();
		}
	}catch(SQLException se){
			se.printStackTrace();  
			return false;
	}
	return true;
 }
 
  public boolean insertCountry (int cid, String name, int height, int population) {
	try {
		sql = connection.createStatement();
		String sqlText;
		sqlText = "SELECT cid FROM a2.country";
		rs = sql.executeQuery(sqlText);
		while(rs.next()){
			int cur = rs.getInt("cid");
			if (cid == cur){
				System.out.println("cid already in table");
				return false;
			}
		}
		sqlText = "INSERT INTO a2.country " + "VALUES (" + cid + ", '" + name + "', " + height + ", " + population + ")";
		int i = sql.executeUpdate(sqlText);
		if (i > 0) {
			return true;
		} else {
			return false;
		}
	} catch (Exception e) {
		e.printStackTrace();
		return false;
	}
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	try {
		sql = connection.createStatement();
		String sqlText;
		
		sqlText = "SELECT cid FROM a2.oceanAccess WHERE oid=" + oid;
		rs = sql.executeQuery(sqlText);
		while(rs.next()){
			count++;
		}
		return count;
	} catch (Exception e) {
		e.printStackTrace();
		return -1;
	} 
  }
   
  public String getOceanInfo(int oid){
  	try {
		sql = connection.createStatement();
		String sqlText;
		
		sqlText = "SELECT oid, oname, depth FROM a2.ocean WHERE oid=" + oid;
		rs = sql.executeQuery(sqlText);
		while(rs.next()){
			int curoid = rs.getInt("oid");
			String curoname = rs.getString("oname");
			int curdepth = rs.getInt("depth");
			if (oid == curoid){
				System.out.println("oid founc");
				return curoid + ":" + curoname + ":" + curdepth;
			}
		}
		return "";
	} catch (Exception e) {
		e.printStackTrace();
		return "";
	} 
  }

  public boolean chgHDI(int cid, int year, float newHDI){
  	try {
		sql = connection.createStatement();
		String sqlText;
		
		sqlText = "UPDATE a2.hdi SET hdi_score=" + newHDI + " WHERE cid=" + cid + "AND year=" + year;
		int i = sql.executeUpdate(sqlText);
		System.out.println(i);
		if (i > 0) {
			return true;
		} else {
			return false;
		}
	} catch (Exception e) {
		e.printStackTrace();
		return false;
	} 
  }

  public boolean deleteNeighbour(int c1id, int c2id){
    try {
		sql = connection.createStatement();
		String sqlText;
		
		sqlText = "DELETE FROM a2.neighbour WHERE country=" + c1id + " AND neighbor=" + c2id;
		int i = sql.executeUpdate(sqlText);
		System.out.println(i);
		
		sqlText = "DELETE FROM a2.neighbour WHERE country=" + c2id + " AND neighbor=" + c1id;
		int f = sql.executeUpdate(sqlText);
		System.out.println(f);
		if (i > 0 || f > 0) {
			return true;
		} else {
			return false;
		}
	} catch (Exception e) {
		e.printStackTrace();
		return false;
	}       
  }
  
  public String listCountryLanguages(int cid){
	try {
		sql = connection.createStatement();
		String sqlText;
		s = "";
		sqlText = "SELECT l.lid, l.lname, (l.lpercentage*c.population) as population FROM a2.country c, a2.language l WHERE c.cid=l.cid AND c.cid=" + cid + " ORDER BY population";
		rs = sql.executeQuery(sqlText);
		while(rs.next()){
			int curlid = rs.getInt("lid");
			String curlname = rs.getString("lname");
			int curpop = rs.getInt("population");
			s = s + "#" + curlid + ":" + curlname + ":" + curpop;
		}
		if (s.isEmpty()) {
			return s;
		}
		temp = s.substring(0, 1);
		if (temp.equals("#")) {
			return s.substring(1);
		} else {
		return s;
		}
		
	} catch (Exception e) {
		e.printStackTrace();
		return "";
	} 
  }
  
  public boolean updateHeight(int cid, int decrH){
     try {
		sql = connection.createStatement();
		String sqlText;
		
		sqlText = "UPDATE a2.country SET height=height-" + decrH + " WHERE cid=" + cid;
		int i = sql.executeUpdate(sqlText);
		System.out.println(i);
		if (i > 0) {
			return true;
		} else {
			return false;
		}
	} catch (Exception e) {
		e.printStackTrace();
		return false;
	} 
  }
    
  public boolean updateDB(){
     try {
		sql = connection.createStatement();
		String sqlText;
		
		sqlText = "CREATE TABLE a2.mostPopulousCountries(cid INTEGER NOT NULL, cname VARCHAR(20) NOT NULL, PRIMARY KEY(cid))";
		int i = sql.executeUpdate(sqlText);
		System.out.println(i);
		
		sqlText = "INSERT INTO a2.mostPopulousCountries(cid, cname) SELECT cid, cname FROM a2.country WHERE population > 100000000 ORDER BY cid ASC";
		int f = sql.executeUpdate(sqlText);
		System.out.println(f);
		if (i >= 0 && f > 0) {
			return true;
		} else {
			return false;
		}
		
	} catch (Exception e) {
		e.printStackTrace();
		return false;
	}     
  }
 /*
  public static void main(String[] args) {
	Assignment2 ass = new Assignment2();
	boolean b = ass.connectDB("jdbc:postgresql://localhost:5432/csc343h-g3benhu", "g3benhu", ""); 
	System.out.println("connect " + b);
	b = ass.insertCountry(4, "name", 2, 3);
	System.out.println("insert " + b);
	int a = ass.getCountriesNextToOceanCount(1);
	System.out.println("ocean " + a);
	String s = ass.getOceanInfo(1);
	System.out.println("oceaninfo " + s);
	s = ass.getOceanInfo(0);
	System.out.println("oceaninfo " + s);
	b = ass.chgHDI(1, 1, 2);
	System.out.println("chghdi1 " + b);
	b = ass.chgHDI(0, 1, 2);
	System.out.println("chghdi2 " + b);
	b = ass.chgHDI(2, 1, 0);
	System.out.println("chghdi3 " + b);
	b = ass.deleteNeighbour(5, 4);
	System.out.println("delneigh1 " + b);
	b = ass.deleteNeighbour(1, 2);
	System.out.println("delneigh2 " + b);
	b = ass.deleteNeighbour(2, 1);
	System.out.println("delneigh3 " + b);
	b = ass.deleteNeighbour(4, 5);
	System.out.println("delneigh4 " + b);
	s = ass.listCountryLanguages(1);
	System.out.println(s);
	s = ass.listCountryLanguages(2);
	System.out.println(s);
	s = ass.listCountryLanguages(3);
	System.out.println(s);
	s = ass.listCountryLanguages(17);
	System.out.println(s);
	
	b = ass.updateHeight(1, 1);
	System.out.println("updateheight1 " + b);
	b = ass.updateHeight(1, 0);
	System.out.println("updateheight1 " + b);
	b = ass.updateHeight(4, 1);
	System.out.println("updateheight1 " + b);
	b = ass.updateHeight(4, 0);
	System.out.println("updateheight1 " + b);
	b = ass.updateHeight(5, 0);
	System.out.println("updateheight1 " + b);
	b = ass.updateHeight(6, 0);
	System.out.println("updateheight1 " + b);
	
	b = ass.updateDB();
	System.out.println("updatedb1 " + b);
	
	
	b = ass.disconnectDB();
	System.out.println("disconenct " + b);
  }
  */
  
}
