import java.sql.*;

public class Assignment2 {
  
 // A connection to the database  
 Connection connection; 
  	
 // Prepared Statement
 PreparedStatement ps;
 PreparedStatement ps2; 
  
 // Resultset for the query
 ResultSet rs;
 ResultSet rs2;	

 //CONSTRUCTOR
 Assignment2(){
	  
 	try { 
		// Load JDBC driver
                Class.forName("org.postgresql.Driver");

        } catch (ClassNotFoundException e) {}

}
  
 //Using the input parameters, establish a connection to be used for this session. Returns true if connection is successful
 
 public boolean connectDB(String URL, String username, String password){
	boolean result = false;	
  	try {
		//Make the connection to the database, ****** but replace "username" with your username ******
		connection = DriverManager.getConnection(URL, username, password);
		result = true;			
	} catch (SQLException e) {}
	finally {
	return result;
	}
  }
  
  //Closes the connection. Returns true if closure was successful
  public boolean disconnectDB() {
	boolean result = false;	
	try{
		if (ps != null) {ps.close();}
		if (rs != null) {
        rs.close();
    }
	
		if (connection != null) {
        	connection.close();
		result = true;	
    } 	
    
  } 	catch (Exception e) {}
	finally {		
	return result;
}
}
    
  public boolean insertCountry (int cid, String name, int height, int population) {
  	boolean result = false;
	
	try {
	
		String instance = "INSERT INTO a2.country VALUES (?, ?, ?, ?)"; 	
		ps = connection.prepareStatement(instance);
		ps.setInt(1, cid);
		ps.setString(2, name);
		ps.setInt(3, height);
		ps.setInt(4, population);
		ps.executeUpdate();
		result = true;
		ps.close();


}	catch (SQLException e) {}
	finally {
		return result; 
		  
	}
}
  
  public int getCountriesNextToOceanCount(int oid) {
	int result = -1;
	try{
		String sql = "SELECT a2.OceanAccess.oid from a2.OceanAccess where a2.OceanAccess.oid = ?";
		ps = connection.prepareStatement(sql);
		ps.setLong(1, oid);
		
		rs = ps.executeQuery();
		if (rs.next()){
			
			try {
				String sql2 = "SELECT COUNT(a2.oceanAccess.cid) as oceancount FROM a2.oceanAccess WHERE a2.oceanAccess.oid = ?;";
				ps2 = connection.prepareStatement(sql2);

				ps2.setLong(1, oid);

				rs2 = ps2.executeQuery();
				
					
				
					while (rs2.next()) {

						result = rs2.getInt("oceancount");
					}	   
				ps2.close();
				rs2.close();	
 
 			 } catch (SQLException e) {}
		}
		ps.close();
		rs.close();
	}
	catch (SQLException e) {}
	finally {
		return result; 
}
}   
  public String getOceanInfo(int oid){
   
 	String result = "";

	try {
		String sql = "SELECT a2.ocean.oid as rsoid, a2.ocean.oname as rsoname, a2.ocean.depth as rsdepth from a2.ocean where a2.ocean.oid=?";

		ps = connection.prepareStatement(sql);
		ps.setLong(1,oid);
		rs = ps.executeQuery();
		while (rs.next()){

	
			String rsoname = rs.getString("rsoname");
			String rsdepth = String.valueOf(rs.getInt("rsdepth"));				
		result = oid + ":" + rsoname + ":" + rsdepth;}
		ps.close();
		rs.close();	
	} catch (SQLException e) {
}
	finally { return result;
}	
 
 }


  public boolean chgHDI(int cid, int year, float newHDI){
   	boolean result = false;

	try {
		String sql = "SELECT a2.hdi.cid, a2.hdi.year from a2.hdi where a2.hdi.cid = ? and a2.hdi.year = ?";
		ps = connection.prepareStatement(sql);
		ps.setInt(1, cid);
		ps.setInt(2, year);
		rs = ps.executeQuery();
		if (rs.next()) { 

			try {
			String sql2 = "UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? and  year  = ?";

			ps2 = connection.prepareStatement(sql2);
			ps2.setFloat(1, newHDI);
			ps2.setInt(2, cid);
			ps2.setInt(3, year);
			ps2.executeUpdate();
			result = true;
			ps2.close();
		
			}
			catch (SQLException e){}
		}
		ps.close();
		rs.close();
	} catch (SQLException e) {}
	finally {return result;}  
  }

  public boolean deleteNeighbour(int c1id, int c2id){
    	boolean result =  false; 

	try {
		String sql = "SELECT a2.neighbour.country, a2.neighbour.neighbor from a2.neighbour where a2.neighbour.country = ? and a2.neighbour.neighbor = ?";
		ps = connection.prepareStatement(sql);
		ps.setInt(1, c1id);
		ps.setInt(2, c2id);
		rs = ps.executeQuery();
		if (rs.next()) {
		
			try{
			String sql2 = "DELETE from a2.neighbour where (country = ? or country = ?) and (neighbor = ? or neighbor = ?)";
			ps2 = connection.prepareStatement(sql2);
			ps2.setInt(1, c1id);
			ps2.setInt(2,c2id);
			ps2.setInt(3, c2id);
			ps2.setInt(4, c1id);
			ps2.executeUpdate();
			result = true;
			ps2.close();
			}catch (SQLException e){}
		}
		ps.close();
		rs.close();
	} catch (SQLException e) {}
	finally {return result;}      
  }
  
  public String listCountryLanguages(int cid){
	String result = "";
	try {	
		String sqltable = "SELECT lid, lname, lpercentage * country.population as population from a2.country, a2.language where country.cid = language.cid and country.cid = ? order by population";

		ps = connection.prepareStatement(sqltable);
		ps.setInt(1, cid);
		rs = ps.executeQuery();
		while (rs.next()) {
			result += String.valueOf(rs.getInt("lid")) + ":" + rs.getString("lname") + ":" + String.valueOf(rs.getFloat("population"));
			result += "#";
		
		}
		if (result != "") {
			result = result.substring(0, result.length() - 1);	 
		}
		ps.close();
		rs.close();
	} catch (SQLException e) {}
	finally{return result;}


  }
  
  public boolean updateHeight(int cid, int decrH){
    	boolean result = false;
	try{
		String sql = "SELECT a2.country.cid FROM a2.country WHERE a2.country.cid = ? ";
		ps = connection.prepareStatement(sql);
                ps.setInt(1, cid);
                rs = ps.executeQuery();
                if (rs.next()) {
			try{
				String sql2 = "UPDATE a2.country SET height = height - ? WHERE cid = ?";
				ps2 = connection.prepareStatement(sql2);
				ps2.setInt(1,decrH);
				ps2.setInt(2,cid);
				ps2.executeUpdate();
				result = true;
				ps2.close();
			}catch (SQLException e){}
		}
		ps.close();
		rs.close();
	} catch (SQLException e) {}	
	finally{return result;}
}
    
  public boolean updateDB(){
	boolean result = false;
	
	try {	
	String sql = "drop table if exists a2.mostPopulousCountries";
	ps = connection.prepareStatement(sql);
	ps.executeUpdate();
	ps.close();
	} catch (SQLException e) {}

	try {
	String sql1 = "CREATE TABLE a2.mostPopulousCountries (cid INTEGER, cname VARCHAR(20))";	
	ps = connection.prepareStatement(sql1);
	ps.executeUpdate();
	ps.close();
	 
	String sql2 = "INSERT INTO a2.mostPopulousCountries (SELECT cid, cname FROM a2.country where population > 100000000 order by cid asc)";
	ps = connection.prepareStatement(sql2);
	ps.executeUpdate();
	ps.close();



	result = true;
	}
	
	catch (SQLException e) {}
	finally {return result;}
	 		 			
 
  }
  public static void main(String[] arg){
	//Assignment2 a2 = new Assignment2();
	//a2.connectDB("jdbc:postgresql://localhost:5432/csc343h-c2gekath", "c2gekath", "");
	//System.out.println(a2.insertCountry(250, "Kathy", 2, 3)); 
	//System.out.println(a2.getCountriesNextToOceanCount(7));
	//System.out.println(a2.getOceanInfo(1));
	//System.out.println(a2.chgHDI(1,2014,3000000));
	//System.out.println(a2.chgHDI(1, 2013, 2));
	//System.out.println(a2.deleteNeighbour(1,2));
	//System.out.println(a2.deleteNeighbour(3, 120));
	//System.out.println(a2.listCountryLanguages(1));		
	//System.out.println(a2.updateHeight(1,3));
	//System.out.println(a2.updateHeight(300, 2));
	//a2.updateDB();
	//a2.disconnectDB();
	
}

}
