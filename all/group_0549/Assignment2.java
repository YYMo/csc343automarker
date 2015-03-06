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
	//System.out.println("----------PostgredSQL JDBC Connection Testing------------");
	try{
		//load jdbc driver
		Class.forName("org.postgresql.Driver");
	}
	catch(ClassNotFoundException e){
		//System.out.println("Fail to find the JDBC driver");
		//e.printStackTrace();
		return;
	}

	System.out.println("driver connected");
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try{      
 	connection = DriverManager.getConnection(URL,username,password);
	return true;
      }
      catch(SQLException e){
	//System.out.println("Connection fail");
	//e.printStackTrace();      
	return false;
      }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try{      
 	connection.close();
	return true;
      }
      catch(SQLException e){
	//System.out.println("Disconnect fail");
	//e.printStackTrace();      
	return false;
      } 
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
   try{
   String sqltext="INSERT INTO a2.country VALUES(?,?,?,?)";
   ps = connection.prepareStatement(sqltext);
   ps.setInt(1,cid);
   ps.setString(2,name);
   ps.setInt(3,height);
   ps.setInt(4,population);
   ps.executeUpdate();
   ps.close();
   return true;
   }
   
   catch(SQLException e){
   //System.out.println("insert into contry fail");
   //e.printStackTrace();
   return false;
   }
 

  }
  
  public int getCountriesNextToOceanCount(int oid) {
		
	try{
		int count=0;
		String sqltext="select count(cid) from a2.oceanAccess where oid=? group by oid";
        	ps = connection.prepareStatement(sqltext);
		ps.setInt(1,oid);
        	rs=ps.executeQuery();
		while(rs.next()){
				count=rs.getInt("count");
				}
		ps.close();
		rs.close();
		return count;
	}

	catch(SQLException e){
   		//System.out.println("count fail");
   		//e.printStackTrace();
   		return -1;
   }	
  }
   
  public String getOceanInfo(int oid){
   String output = "";   
   try{
   	String sqltext="SELECT * FROM a2.ocean WHERE oid= ?";
	ps = connection.prepareStatement(sqltext);
   	ps.setInt(1,oid);	
   	rs = ps.executeQuery();
   	while(rs.next()){
		String oname = rs.getString("oname");
		int depth = rs.getInt("depth");
		output = (oid+":"+oname+":"+depth+"\n");	
   	}
	ps.close();
	rs.close();
   }
   
   catch(SQLException e){
   //System.out.println("getoceaninfo fail");
   //e.printStackTrace();
   }
	return output;

  }

  public boolean chgHDI(int cid, int year, float newHDI){
	try{
   		String sqltext="UPDATE a2.hdi SET hdi_score=? where cid=? and year=?";
   		ps = connection.prepareStatement(sqltext);
   		ps.setFloat(1,newHDI);
   		ps.setInt(2,cid);
   		ps.setInt(3,year);
   		if(ps.executeUpdate() == (int)0) return false;
   		ps.close();
   		return true;
   	}
   
   	catch(SQLException e){
   		//System.out.println("updata hdi fail");
   		//e.printStackTrace();
   		return false;
   	}
  }

  public boolean deleteNeighbour(int c1id, int c2id){
   int result1=0;
   int result2=0;
   try{
   String sqltext="DELETE from a2.neighbour WHERE country=? and neighbor=?";
   ps = connection.prepareStatement(sqltext);
   ps.setInt(1,c1id);
   ps.setInt(2,c2id);
   if(ps.executeUpdate() == (int)0) return false;
   ps.close();
   sqltext="DELETE from a2.neighbour WHERE country=? and neighbor=?";
   ps = connection.prepareStatement(sqltext);
   ps.setInt(1,c2id);
   ps.setInt(2,c1id);
   if(ps.executeUpdate() == (int)0) return false;
   ps.close();
   return true;
   }
   
   catch(SQLException e){
   //System.out.println("delete neighbour fail");
   //e.printStackTrace();
   return false;
   }               
  }
  
  public String listCountryLanguages(int cid){
	
	String result ="";
	try{
		String sqltext="SELECT language.lid as lid, language.lname as lname,country.population * language.lpercentage as population FROM a2.language,a2.country WHERE country.cid=language.cid and country.cid= ?";	
   		ps = connection.prepareStatement(sqltext);
   		ps.setInt(1,cid);
		rs = ps.executeQuery();
		while(rs.next()){
			int lid = rs.getInt("lid");
			String lname=rs.getString("lname");
			float population=rs.getFloat("population");
			result=result+lid+":"+lname+":"+population+"#";
		}
		ps.close();
		rs.close();
	}
	catch(SQLException e){
   		//System.out.println("GETCOUNTRYLANGUAGEFAIL");
   		//e.printStackTrace();
   	}	
	return result;
  }
  
  public boolean updateHeight(int cid, int decrH){
   try{
   int height=0;
   String sqltext="SELECT * FROM a2.country WHERE cid=?";
   ps = connection.prepareStatement(sqltext);
   ps.setInt(1,cid);
   rs = ps.executeQuery();
   	while(rs.next()){
		height = rs.getInt("height");
   	}
   ps.close();
   int newheight=height-decrH;
   sqltext="UPDATE a2.country SET height=? WHERE cid=?";
   ps = connection.prepareStatement(sqltext);
   ps.setInt(1,newheight);
   ps.setInt(2,cid);
   if(ps.executeUpdate() == (int)0) return false;
   ps.close();
   rs.close();
   return true;
   }
   
   catch(SQLException e){
   //System.out.println("update height fail");
   //e.printStackTrace();
   return false;
   }
  }
    
  public boolean updateDB(){
   try{
	sql = connection.createStatement();
	String sqltext="CREATE TABLE IF NOT EXISTS a2.mostPopulousCountries(cid int, cname varchar(20))";
	sql.executeUpdate(sqltext);
	sqltext="DELETE FROM a2.mostPopulousCountries";
	sql.executeUpdate(sqltext);

        sqltext="SELECT * FROM a2.country WHERE population>100000000 ORDER by cid ASC";
   	rs = sql.executeQuery(sqltext);
	sqltext="INSERT INTO a2.mostPopulousCountries VALUES(?,?)";
	ps = connection.prepareStatement(sqltext);
	if(rs!=null){
   	   while(rs.next()){
	   	int cid = rs.getInt("cid");
	   	String cname = rs.getString("cname");
		ps.setInt(1,cid);
		ps.setString(2,cname);
		ps.executeUpdate();
   	   }
	}
	ps.close();
	rs.close();
	return true;
   }
   
   catch(SQLException e){
   //System.out.println("updateDB fail");
   //e.printStackTrace();
   return false;  
   }     
  }
  
}
