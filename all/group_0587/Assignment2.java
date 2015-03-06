// Instructions:
// 1) Connect to "ssh dbsrv1.cdf.toronto.edu" using your cdf username and password
// 2) Download the JDBC driver (version 9.1-903 JDBC 4) from http://jdbc.postgresql.org/download.html and 
//    copy jdbc jar file (using sftp) to dbsrv1 server.
// 3) On line 60 connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/csc343h-username", "username", "");
//    -leave as is the host and port number ("localhost:5432"); 
//    -replace "username" with your cdf username, where the fields "csc343h-username" is the database name and "username" is the username that will be used to login into the database
//    - you may need to set the password field. The default one is set to empty (""); 
// 4) Compile the code:
//         javac JDBCExample.java
// 5) Run the code:
//         java -cp /*****path-to-jdbc-directory*****/postgresql-9.1-903.jdbc4.jar:. JDBCExample   
//    where postgresql-9.1-903.jdbc4.jar is jdbc jar file downloaded in step 2




import java.sql.*;
import java.lang.*;

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
		
 			// Load JDBC driver
			Class.forName("org.postgresql.Driver");
 
		} catch (ClassNotFoundException e) {
 
			//e.printStackTrace();
			return;
 
		}
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){

  		try {
		
 			//connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/csc343h-c4wangru", "c4wangru", "LEO123--qwedsa");
 			connection = DriverManager.getConnection(URL, username, password);
		
			

		} catch (SQLException e) {

			//e.printStackTrace();
			return false;
 
		}
      return true;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
  	try {
  		connection.close();
  		return true;
  	} catch(SQLException e) {
  		//e.printStackTrace();
  		return false;
  	}   
  }

  public boolean insertCountry (int cid, String name, int height, int population) {
  	try{
  		sql = connection.createStatement(); 

			//---------------------------------------------------------------------------------------
			//Create jdbc_demo table
		String sqlText, sqlText2;
		
		//System.out.println("Executing this command: \n" + sqlText.replaceAll("\\s+", " ") + "\n");
		/*sqlText2 = ""
                    	+ "SET SEARCH_PATH TO A2;";
		sql.executeUpdate(sqlText2);*/
		sqlText =""
			+ "select * from a2.country "
                    	+ "WHERE cid= "+ cid+";";
		rs=sql.executeQuery(sqlText);
		if(rs.next()!=true){

		
			sqlText = ""
		           	+ "INSERT INTO a2.country "
		            	+ "VALUES     (" + cid + ", " +"'" + name +"'" + ", " + height + "," + population + ")";
		   		 //System.out.println("Executing this command: \n" + sqlText.replaceAll("\\s+", " ") + "\n");
		    	sql.executeUpdate(sqlText);
			rs.close();  
			return true;
		}
		else {
			rs.close();	
			return false;
		}
  	}catch(SQLException e){
  		//e.printStackTrace();
  		return false;
  	}
   
  }
  
  public int getCountriesNextToOceanCount(int oid) {
  	int returnvalue=-1;
  	String temp;
  	try{
	  	sql = connection.createStatement(); 
		/*String sqlText2;
		sqlText2 = ""
                    	+ "SET SEARCH_PATH TO A2;";
		sql.executeUpdate(sqlText2);*/
				//---------------------------------------------------------------------------------------
				//Create jdbc_demo table
		String sqlText;
		sqlText ="SELECT count(cid) AS count FROM a2.oceanAccess WHERE oid =" + oid+ ";";
			//System.out.println("Executing this command: \n" + sqlText.replaceAll("\\s+", " ") + "\n");
		
	    	rs=sql.executeQuery(sqlText);
	  	if (rs==null)
	  	{
	  		return -1;
	  	}
	  	else
	  	{
	  		while(rs.next())
	  		{
				returnvalue=rs.getInt("count");

	  		}
	  		rs.close();
			return returnvalue;
	  	}
	  }catch (SQLException e) {
            //e.printStackTrace();
            return -1;
        }
  }
   
  public String getOceanInfo(int oid){
  	String returnvalue="";
  	try{
	  	sql = connection.createStatement(); 

				//---------------------------------------------------------------------------------------
			
		//Create jdbc_demo table
		String sqlText;

		sqlText = "SELECT oid,oname, depth FROM a2.ocean "
		        + "WHERE oid =" + oid+ ";";
			
	    	rs=sql.executeQuery(sqlText);
	  	if (rs.next()==false)
	  	{
	  		return "";
	  	}
	  	else
	  	{
	  		while(rs.next())
	  		{
				returnvalue=rs.getString("oid")+":"+rs.getString("oname")+":"+rs.getString("depth");

	  		}
	  		rs.close();
			return returnvalue;
	  	}
	  }catch (SQLException e) {
            //e.printStackTrace();
            return "";
        }
  }

  public boolean chgHDI(int cid, int year, float newHDI){
 	String returnvalue="";
  	try{
	  	sql = connection.createStatement(); 
		String sqlText2="";
		sqlText2 = "SELECT * FROM a2.hdi "
		        + "WHERE cid=" + cid+ ";";
		rs=sql.executeQuery(sqlText2);
		if(rs.next()==false)
		{
			rs.close();
			return false;
		}

				//---------------------------------------------------------------------------------------
				//Create jdbc_demo table
		else{
			String sqlText="";
	 		sqlText = "UPDATE a2.hdi      " 
		                + "   SET hdi_score ="+ newHDI
		                + " WHERE  cid = " + cid
		                + " and year=" + year; 

			sql.executeUpdate(sqlText);
			rs.close();
			return true;
		}
	}catch(SQLException e) {
            //e.printStackTrace();
            return false;
        }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
  try{
	  	sql = connection.createStatement(); 

				//---------------------------------------------------------------------------------------
				//Create jdbc_demo table
		String sqlText;
 		sqlText = "DELETE from a2.neighbour      " 
                        + " WHERE  country = " + c1id
                        + " and neighbor=" + c2id; 

		sql.executeUpdate(sqlText);
		String sqlText2;
 		sqlText2 = "DELETE from a2.neighbour      " 
                        + " WHERE  country = " + c2id
                        + " and neighbor=" + c1id; 

		sql.executeUpdate(sqlText2);
		return true;
	}catch(SQLException e) {
            //e.printStackTrace();
            return false;
        }       
  }
  
  public String listCountryLanguages(int cid){
	String returnval="";
    	int pop=0;
   	int i=1;
	double per=0d;
    	try{
         	 sql = connection.createStatement();

                //---------------------------------------------------------------------------------------
                //Create jdbc_demo table
       		String sqlText="";
        	sqlText = "SELECT population FROM a2.country"
                    	    + " WHERE cid="+ cid+";";
		rs=sql.executeQuery(sqlText);
		if(rs.next()){
			pop=rs.getInt("population");
			//System.out.print(pop);
			rs.close();
			sqlText ="";
			sqlText = "SELECT lid,lname,lpercentage FROM a2.language  "
				+ " WHERE  cid=" + cid+";";
			rs=sql.executeQuery(sqlText);
		}
		
		while(rs.next()){
			
		        double population =(double)(pop * rs.getDouble("lpercentage"));
			//System.out.print(population);
		        returnval+=rs.getString("lid")+":"+rs.getString("lname")+":"+population+"#";
		        i++;
		    }
		rs.close();
		return returnval;
    	}catch(SQLException e) {
            	//e.printStackTrace();
           	 return "";
        }       
  }
  
  public boolean updateHeight(int cid, int decrH){
  try{
	  	sql = connection.createStatement(); 
		String sqlText2="";
		sqlText2 = "SELECT * FROM a2.country "
		        + "WHERE cid=" + cid+ ";";
		rs=sql.executeQuery(sqlText2);
		if(rs.next()==false || decrH <0)
		{
			rs.close();
			return false;
		}

				//---------------------------------------------------------------------------------------
				//Create jdbc_demo table
		else{
			String sqlText;
	 		sqlText = "UPDATE a2.country     " 
		                + "   SET height = height-"+ decrH
		                + " WHERE  cid = " + cid;
			sql.executeUpdate(sqlText);
			rs.close();
			return true;
		}
	}catch(SQLException e) {
            //e.printStackTrace();
            return false;
        } 
  }
    
  public boolean updateDB(){
	try{
	  	sql = connection.createStatement(); 

				//---------------------------------------------------------------------------------------
				//Create jdbc_demo table
		String sqlText2="";
		sqlText2 = "SELECT table_name FROM information_schema.tables WHERE table_schema='a2' and UPPER(table_name) = 'MOSTPOPULOUSCOUNTRIES'; ";
		rs=sql.executeQuery(sqlText2);
		if(rs.next()==false){
			
			String sqlText;
		
		 	sqlText = " CREATE TABLE a2.mostPopulousCountries AS("
				+ " SELECT cid, cname from a2.country"
				+ " WHERE  population>100000000 ORDER BY cid);" ;
					
			sql.executeUpdate(sqlText);
			
			rs.close();
			return true;
		}
		
			String sqlText;
			sqlText = "DROP TABLE a2.mostPopulousCountries;";
			sql.executeUpdate(sqlText);
			sqlText ="";
			sqlText = " CREATE TABLE a2.mostPopulousCountries AS("
				+ " SELECT cid, cname from a2.country"
				+ " WHERE  population>100000000 ORDER BY cid);" ;
					//System.out.println("Executing this command: \n" + sqlText.replaceAll("\\s+", " ") + "\n");
			sql.executeUpdate(sqlText);
			
			rs.close();
			return true;

	}catch(SQLException e) {
           // e.printStackTrace();
            return false;
        } 
  }
	
 
  
}
