import java.sql.*;

//this is test

public class Assignment2 {
      	
	// A connection to the database  
	private Connection connection = null;
	  
	// Statement to run queries
	public Statement sql=null;
	  
	// Prepared Statement
	private PreparedStatement ps = null;
	  
	// Resultset for the query
	private ResultSet rs;
	  
	// Constructor
	public void Assignment2(){
		//System.out.println("----PostgreSQL JDBC Connection Testing------ ");
		try{
		  		//Load JDBC driver
		  		Class.forName("org.postgresql.Driver"); 	
		}catch (ClassNotFoundException e){
		  		//System.out.println("Please include your PostgerSQl JDBC Driver in the Library path!");
		  		//e.printStackTrace();
		}
		return;
		//System.out.println("PostgreSQl JDBC Driver Registered!\n");
	}	 
	  
	//Using the input parameters, establish a connection to be used for this session. Returns true if 				connection is sucessful
	public boolean connectDB(String URL, String username, String password){
		try{
  			//Make the connection to the database
			//System.out.println("Please input your name");
			//System.out.println("URL-----------"+URL +username +password + "\n");
			//connection= DriverManager.getConnection("jdbc:postgresql://localhost:5432/csc343h-c4daihe","c4daihe","");
  			connection= DriverManager.getConnection(URL,username,password);  			
  			/*TODO: check statement location*/
  			sql= connection.createStatement();
  			return true;
  		}catch(SQLException e){
		  		//e.printStackTrace();
		}
		return false;
	}

	//Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB(){
		try {
			if (rs!=null)
        		rs.close();
        	if (ps!=null)
        		ps.close();
        	if (connection!=null)
        		connection.close();
        	//System.out.println("disconnect from database");
        	return true;
        }catch (SQLException e){
        	//e.printStackTrace();
        }
        return false;
	}

	public boolean insertCountry (int cid, String name, int height, int population) {
		try{
			//System.out.println("Using prepared statement to insert into the country table\n");
	  		String sqlText;
			sqlText = "Insert into country\n" + "Values (?,?,?,?)";
	  		//System.out.println("Prepared statement:\n" + sqlText.replaceAll("\\s+", " ") + "\n");
	  		ps = connection.prepareStatement(sqlText);
	  		ps.setInt(1,cid);
	  		ps.setString(2, name);
	  		ps.setInt(3,height);
	  		ps.setInt(4, population);
	  		if(ps.executeUpdate()==0){
	  			ps.close();
	  			return false;
	  		}else{
	  			ps.close();
				return true;
			}
		}catch(SQLException e){
  			//System.out.println("Query Execution Failed!");
  			//e.printStackTrace();
		}
  			return false;
	}	
  
  	public int getCountriesNextToOceanCount(int oid) {
  		int count = 0;

  		String sqlText = "SELECT * FROM oceanAccess WHERE oid =" + oid;
  		try{
  			sql =connection.createStatement();
  			rs = sql.executeQuery(sqlText);
  			if(rs.next()){
  				sqlText = "SELECT count(cid) AS num FROM oceanAccess WHERE oid =" + oid;
  				sql =connection.createStatement();  				
  				rs = sql.executeQuery(sqlText);
  				if(rs.next()){
  					count = rs.getInt("num");
  					rs.close();
  					return count;}
  				else{
  					rs.close();
  					return -1;
  				}
  			}
  			else{
  				rs.close();
  				//System.out.println("Error!No country next to this ocean!\n");
  				return -1;
  			}
  		}catch(SQLException e){}
  		return -1;  
  	}
   
	  public String getOceanInfo(int oid){
	  	String sqlText = "SELECT oid, oname, depth FROM ocean WHERE oid =" + oid;
	  	String info;
	  	try{
	  		sql =connection.createStatement();
  			rs = sql.executeQuery(sqlText);
	  		if(rs.next()){
	  			info = rs.getInt("oid") + ":" + rs.getString("oname") + ":" + rs.getInt("depth");
	  			rs.close();
	  			return info;
	  		}
	  		else{
	  			rs.close();
	  			//System.out.println("ocean can not be found in the ocean table");
	  			return "";
	  		}
	  	}catch(SQLException e){}
	   	return "";
	  }

  	public boolean chgHDI(int cid, int year, float newHDI){
  		String sqlText = "UPDATE hdi SET hdi_score='"+newHDI+"' WHERE year="+ year +"AND cid="+cid+";";
  		try{
  			sql =connection.createStatement();
  			if(sql.executeUpdate(sqlText)==0){
  				return false;
  			}
  			else{
  				return true;
  			}
  		}catch(SQLException e){} 
		return false;  
  	}

	public boolean deleteNeighbour(int c1id, int c2id){
		String sqlText = "DELETE FROM neighbour WHERE country=" + c1id + " AND neighbor="+ c2id;
		try{
			sql =connection.createStatement();
  			if(sql.executeUpdate(sqlText)!=0){
  				sqlText = "DELETE FROM neighbour WHERE country=" + c2id + " AND neighbor="+ c1id;
  				sql =connection.createStatement();
  				if(sql.executeUpdate(sqlText)!=0){
  					return true;
  				}else{
  				return false;}
  			}else{
  				return false;
  			}
  		}catch(SQLException e){
  			//e.printStackTrace();			
		} 
		return false;         
  	}
  
  	public String listCountryLanguages(int cid){
  		String sqlText = "select l.lid, l.lname, c.population*l.lpercentage AS population from country c, language l where l.cid="+ cid + " AND c.cid="+cid+"order by population;"; 
  		String str="";
  		
 		try{
			sql =connection.createStatement();
  			rs = sql.executeQuery(sqlText);
  			if(rs.next()){
  				str=rs.getInt("lid")+":"+rs.getString("lname")+":"+ rs.getFloat("population");
  				while(rs.next())
  				{
  					str=str+"#"+rs.getInt("lid")+":"+rs.getString("lname")+":"+ rs.getFloat("population");
  				}
  				return str;
  			}else{
  				return "";
  			}
  		}catch(SQLException e){
	  	}
		return "";
  	}
  
  public boolean updateHeight(int cid, int decrH){
  	String sqlText = "update country set height=height-"+decrH+" where cid IN (select cid from country where cid="+cid+");";
  	try{
			sql =connection.createStatement();
  			if(sql.executeUpdate(sqlText)==0)
  			{
  				return false;
  			}else{
  				return true;
  			}
  		}catch(SQLException e){
  			//e.printStackTrace();			
		} 
		return false; 
  }
    
  	public boolean updateDB(){
  		String createtable="CREATE TABLE mostPopulousCountries ( cid INTEGER NOT NULL, cname VARCHAR(20) NOT NULL);";
  		String update="insert into mostpopulouscountries VALUES(?,?);";
  		String sqlText="select cid, cname from country where population>=10e8 order by cid ASC;";
  		try {
      		sql=connection.createStatement();
      		sql.executeUpdate(createtable);
      		rs=sql.executeQuery(sqlText);
      		ps=connection.prepareStatement(update);
      		if (rs != null){
        		while(rs.next()){
		      		ps.setInt(1,rs.getInt("cid"));
		      		ps.setString(2,rs.getString("cname"));
		      		ps.executeUpdate();
        		}
      		}
      		else{
      			return false;
      		}
      		return true;
    	} catch (SQLException e) {
      		//e.printStackTrace();
    	}
		return false;    
  	}
  
}
