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
  
  // Newline operator
  //   static String eol = System.getProperty("line.separator");
  

   //CONSTRUCTOR
  Assignment2(){
     try {
	
	// Load JDBC driver
	Class.forName("org.postgresql.Driver");         
	}          
	catch (ClassNotFoundException e){
         System.exit(1); 
       }    
      } 
   
   

 //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password) {
      
        //Establish a connection to be used
	try { 
	 	connection = DriverManager.getConnection(URL, username, password); 
             }         
         catch (SQLException e) {  
	
		//System.out.println("Connection Failed! Check output console");
        	return false;  
	 } 
	return true; 
	}
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
    
   try {  
	connection.close(); 
        }  
       catch (SQLException e) { 
         return false;  
        } 
     return true;  
     } 

    
  public boolean insertCountry (int cid, String name, int height, int population) {
   
   int output=0;
   String insertCountry = "insert into a2.country (cid,cname,height,population) SELECT ?,?,?,? where not exists (select cid 	   from a2.country where cid = ?)";
  
   try{  
       ps = connection.prepareStatement(insertCountry); 
       
       ps.setInt(1,cid); 
       ps.setString(2,name); 
       ps.setInt(3,height);  
       ps.setInt(4,population); 
       ps.setInt(5,cid);  
     
       output = ps.executeUpdate();  
     }  
     
     catch (SQLException e) { 
       }
   
     //Check to see if update changed anything (added a tuple)
    if (output != 0) {  
        return true ;
       }  
     
    return true;
 
 }
  
  public int getCountriesNextToOceanCount(int oid) {
	
	// number of countries next to oceans returns 0 by default
	int numCountries=0; 
	String nextOcean = "select count(cid) as num from a2.oceanAccess where oid = ?";   
        
	try { 
                ps = connection.prepareStatement(nextOcean); 
         	ps.setInt(1, oid);
                rs = ps.executeQuery();
                numCountries = rs.getInt("count");
       }
          catch (SQLException e) {
	  // error occurred
	  return -1; 
          
	  }    
         
	
	return numCountries; 
  } 

   
  public String getOceanInfo(int oid){
   
	String output = "";
	String oceanInfo = "select oid, oname, depth from a2.ocean where oid = ?";
         
	try { 
	
	ps = connection.prepareStatement(oceanInfo); 	
	ps.setInt(1,oid);
        rs = ps.executeQuery(); 
           
	while(rs.next()){
	
	output.concat(String.valueOf(rs.getInt("oceanId" + ":"))).concat(rs.getString("oceanName" + ":")).concat(String.valueOf       (rs.getInt("oceanDepth")));
	} 
	
	if (!rs.next()){ 
          output = ""; 
                }
	} //try end bracket

	
	// error occurred return the empty string
          catch (SQLException e) { 
                     output="";
                }
    
       return output;

  }

  public boolean chgHDI(int cid, int year, float newHDI){
    
      int output=0;
      String changeHdi = "update a2.hdi set hdi = ? where (cid = ? and year = ?)"; 
	 
      try {

        ps = connection.prepareStatement(changeHdi); 
        ps.setInt(2,cid);
	ps.setInt(3,year); 
	ps.setFloat(1,newHDI);  
      
        output = ps.executeUpdate(); 
      } 
       catch (SQLException e) { 
        } 


     if (output != 0) {         
	return true; 
      }   
   
    return  true;
 
   }//method bracket

  public boolean deleteNeighbour(int c1id, int c2id){
    
	int output=0;
	String deleteN = "delete from a2.neighbour where ((country = ? and neighbor = ?) or (country =? and neighbor = ?))"; 
  
	try { 

	ps = connection.prepareStatement(deleteN); 
        ps.setInt(1,c1id); 
        ps.setInt(2,c2id); 
        ps.setInt(3,c2id); 
        ps.setInt(4,c1id); 
       
	output = ps.executeUpdate(); 
        } 
        
	// error occurred
	catch (SQLException e) { 
          } 
	 
	//we want to check if two tuples have been changed/deleted
	 if (output != 2) { 
	       return true; 
	  } 
	    else { 
	      return false;
	     

	} 
       }
  
  public String listCountryLanguages(int cid){
	
	int population; 
        float populationPercent;
	String output1 = "select population from a2.country where cid = ?"; 
        String output2 = "select lid, lname, lpercentage from a2.language where cid = ? order by lpercentage desc" ; 
        String out = ""; 
        
 
	try { 
          ps = connection.prepareStatement(output1); 
          ps.setInt(1,cid); 
          rs = ps.executeQuery(); 
          population = rs.getInt("population"); 
          

	  ps = connection.prepareStatement(output2);
          ps.setInt(1,cid); 
          rs = ps.executeQuery(); 
               
	  while (rs.next()) {
     		  populationPercent = rs.getInt("lpercentage") * population ;
	          out.concat(String.valueOf(rs.getInt("lid :" ))).concat(rs.getString("lname:")).concat(String.valueOf(populationPercent) ).concat("#"); 
	  
		 out.substring(0 ,out.length()-1);

  	   }  
	  if (!rs.next()){
	       out = ""; 
	          }
            } 
	    
	    //error occured
	    catch (SQLException e) { 
                    return ""; 
	                 } 
            
	  return out; 
        } 


  public boolean updateHeight(int cid, int decrH){
   

   int height;
   int output=0;
   String update = "select height from a2.country where cid = ?"; 
   String update2 = "update a2.country set height = ? where cid = ?";  
   
    
 	try { 
              ps = connection.prepareStatement(update); 
       	      ps.setInt(1, cid);               
	      rs = ps.executeQuery(); 
              height = rs.getInt("height of object"); 
          
	 if(height < decrH){
                 return false; 
                 }

	 if (height >= decrH) {                       
	  
	    height = height - decrH; 
	    
	    ps = connection.prepareStatement(update2); 
	    ps.setInt(1,height); 
	    ps.setInt(2,cid); 
           
	    output = ps.executeUpdate(); 
              }        
        } 
	        
	// error occurred
	 catch (SQLException e) {       
	} 

	if (output != 0){        
	    return true; 
        }
     	
    return true; 
  }//method close
	

 /*   public static void main(String [] args){
  
   //Testing with my cdf account and creating an Assignment2 object
   Assignment2 object = new Assignment2();  
   
   boolean val;
   int value;
   String v = "";
   

   val = object.connectDB("jdbc:postgresql://localhost:5432/csc343h-c4shaikj", "c4shaikj", "");  
   System.out.println("connect value :" + val);  
   
   val = object.disconnectDB();  
   System.out.println("disconnect sucessfull");

   val = object.insertCountry(1, "a", 1, 1); 
   System.out.println("SUCESS : insert country  " + val);

   value = object.getCountriesNextToOceanCount(1);
   System.out.println("countries next to ocean count  : " + val);
 
   v = object.getOceanInfo(2);
   System.out.println("ocean info : " + val);

   val = object.chgHDI(1, 2010, 0.002f);
   System.out.println("change HDI : " + val);

   val = object.deleteNeighbour(1, 2);
   System.out.println("delete Neighbour sucessfull : " + val);

   v = object.listCountryLanguages(1);
   System.out.println("country languages are : " + val);

   val = object.updateHeight(1,5);
   System.out.println("update Height sucessfull: " + val);

   val = object.updateDB();
   System.out.println("update DB sucessfull: " + val);
  
*/






    
  public boolean updateDB(){
	 String updatedb = "create table if not exists popular as (SELECT cid, cname FROM a2.country where population > 100 000 order by cid asc)"; 
       
	try { 
	ps = connection.prepareStatement(updatedb);  
	rs = ps.executeQuery(); 
       }         

	catch (SQLException e) {          
         } 

        return true; 
	   } 
	 }




