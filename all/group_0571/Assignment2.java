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

   } catch(ClassNotFoundException e){
      //System.out.println("Error in Assignment 2 contrusctor");
   } 

 }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){

    try{
      connection = DriverManager.getConnection(URL, username, password);
      return true;

    } catch(SQLException e){
      return false;
    }


   // return false;
  }
  

  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
	try{
	   connection.close();
	   return true;
	}catch(SQLException e){
	   return false;
	} 
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
   try{


      String sqlText = "SELECT * FROM a2.country WHERE cid = ?";
      ps = connection.prepareStatement(sqlText);
      ps.setInt(1, cid);
      

      String worth1 = "";
      rs = ps.executeQuery();

      while(rs.next()){
        worth1 = "" +  rs.getInt(1) + ":" + rs.getString(2) + ":" + rs.getInt(3) + ":" + rs.getInt(4) +"" ;
      }
      
      ps.close();
      rs.close();
      
      if(worth1 == ""){
        String Order;
        Order = "INSERT INTO a2.country values (?,?,?,?)"; 

        ps = connection.prepareStatement(Order);

        ps.setInt(1, cid);
        ps.setString(2, name);
        ps.setInt(3, height);
        ps.setInt(4, population);

	int UpdateReturner = 0;
        UpdateReturner = ps.executeUpdate();

        ps.close();
	if(UpdateReturner != 0) {
           return true;
        }else{
           return false;
        }
      } else{
        return false;
      }

      

  
   } catch(SQLException e){

      return false;
   }



  // return false;
  }
 

  public int getCountriesNextToOceanCount(int oid) {

  	try{

  	  String Order;
  	  Order = "SELECT count(*) " +
        "FROM a2.oceanAccess A " +
  		  "WHERE A.oid = ? ;";

      ps = connection.prepareStatement(Order);
  	  ps.setInt(1, oid);

      int worth = 0;
  	  rs = ps.executeQuery();

  	  while(rs.next()){
         worth = rs.getInt(1);
  	  }
      
      ps.close();
      rs.close();
      
      if(worth == 0){
        return -1;
      } else{
        return worth;
      }
      

  	}catch(SQLException e){

      return -1;
     }

    //return -1;  
  }
  


  public String getOceanInfo(int oid){

	try{
	   
  	   String Order;
  	   Order = "SELECT * FROM a2.ocean A WHERE A.oid = ? ;";

  	   ps = connection.prepareStatement(Order);
  	   ps.setInt(1,oid);
  	 
  	   rs = ps.executeQuery();
  	
  	  String result = "";
  	  while(rs.next()){
  	     result = result + rs.getInt(1) +":" + rs.getString(2) + ":" + rs.getInt(3);

    	  }
  	 ps.close();
     rs.close();

  	 return result; 
	  
	}catch(SQLException e){
    return "";
  }


//   return "";
}

  public boolean chgHDI(int cid, int year, float newHDI){

	try{
	   String Order = "UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? AND year = ? ;";

	   ps = connection.prepareStatement(Order);
	   ps.setFloat(1, newHDI);
	   ps.setInt(2, cid);
	   ps.setInt(3, year);

	   int UpdateReturner = 0;
	   UpdateReturner = ps.executeUpdate();
	   ps.close();
	
	   if(UpdateReturner !=  0){
       return true; 
     }
	   else{
      return false;
     } 
       
	}catch(SQLException e){

       return false;
   }


  // return false;
  }

  public boolean deleteNeighbour(int c1id, int c2id){

	try{
	   String Order = "DELETE FROM a2.neighbour WHERE (country = ? AND neighbor = ?) or (country = ? AND neighbor = ?) ;";

	   ps = connection.prepareStatement(Order);
	   ps.setInt(1, c1id);
	   ps.setInt(2,c2id);
	   ps.setInt(3,c2id);
	   ps.setInt(4,c1id);

	   int DeleteReturner = 0;
	   DeleteReturner = ps.executeUpdate();
	   ps.close();

	   if(DeleteReturner != 0){
      return true;
      }
	   else{
      return false;
    }
 

	} catch(SQLException e){

      return false;
   }


   //return false;        
  }
  
  public String listCountryLanguages(int cid){

	try{
	   String Order = "SELECT A.lid as id, A.lname as lname, (A.lpercentage*B.population) as population FROM a2.language A JOIN a2.country B ON A.cid = B.cid WHERE B.cid = ? ORDER BY (A.lpercentage*B.population) ;";
	
	   ps = connection.prepareStatement(Order);
	   ps.setInt(1, cid);

	   rs = ps.executeQuery();

	
	   String result = "";
	   while(rs.next()){
	     result = result + rs.getInt(1) +":" + rs.getString(2) + ":" + rs.getInt(3) + "#";
	   
  	  }
	   ps.close();
     rs.close();
	   
     if(result!= ""){
	   result = result.substring(0,result.length()-1);
	   }	


	   return result; 
	  
	}catch(SQLException e){
    return "";
   }

//	return "";
  }
  
  public boolean updateHeight(int cid, int decrH){

	try{
	   String Order = "UPDATE a2.country  SET height = height - ? WHERE cid = ? ;" ;

	   ps = connection.prepareStatement(Order);
	   ps.setInt(1, decrH);
	   ps.setInt(2, cid);

	   int UpdateReturner = 0;
	   UpdateReturner = ps.executeUpdate();
	   ps.close();
	
	   if(UpdateReturner !=  0) return true;
	   else return false;
       
	}catch(SQLException e){

        return false;
   }



  //  return false;
  }
    
  public boolean updateDB(){

	try{
	String Order = "DROP TABLE IF EXISTS a2.mostPopulousCountries CASCADE; "   + " CREATE TABLE a2.mostPopulousCountries ( cid INTEGER , cname VARCHAR(20)); "+  " INSERT INTO a2.mostPopulousCountries ( SELECT cid, cname FROM a2.country WHERE population > 100000000 ORDER BY cid ASC ) ; " ;

	
	sql = connection.createStatement();
	sql.executeUpdate(Order);
	return true;


	}catch(SQLException e){

        return false;
   }




//	return false;    
  }
  
}

