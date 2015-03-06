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
	}

	catch(ClassNotFoundException except) {
		System.exit(1);
	}
  }
  
/*
// TESTER
public static void main (String args[]) {

Assignment2 a2 = new Assignment2();

if (a2.connectDB("jdbc:postgresql://localhost:5432/csc343h-c4mahmoo", "c4mahmoo", ""))

System.out.println("connected!");

//insertCountry

System.out.println(a2.insertCountry(25, "test25", 25, 25));

System.out.println(a2.insertCountry(100, "test50", 50, 50));

System.out.println(a2.insertCountry(100, "test100", 100, 100));

// getCountriesNextToOceanCount

System.out.println(a2.getCountriesNextToOceanCount(5));

System.out.println(a2.getCountriesNextToOceanCount(10));

System.out.println(a2.getCountriesNextToOceanCount(15));

// getOceanInfo

System.out.println(a2.getOceanInfo(1));

System.out.println(a2.getOceanInfo(10));

System.out.println(a2.getOceanInfo(20));

//chgHDI

System.out.println(a2.chgHDI(5, 2010, 0.8f));

System.out.println(a2.chgHDI(500, 2008, 0.8f));

System.out.println(a2.chgHDI(5000, 2006, 0.8f));

//deleteNeighbour

System.out.println(a2.deleteNeighbour(1, 2));

System.out.println(a2.deleteNeighbour(3, 4));

//listCountryLanguages

System.out.println(a2.listCountryLanguages(2));

System.out.println(a2.listCountryLanguages(0));

//updateHeight

System.out.println(a2.updateHeight(1, 2));

System.out.println(a2.updateHeight(10, 100));

//updateDB

System.out.println(a2.updateDB());

if (a2.disconnectDB())

System.out.println("disconnected!");

}
*/

//Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	try {
		connection = DriverManager.getConnection(URL, username, password);
		return true;
  	}

	catch(SQLException except) {
		return false;
  	}
  }

  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
	try {
		connection.close();
		return true;
	}

	catch(SQLException except) {
		return false;    
	}  
}
    
  public boolean insertCountry (int cid, String name, int height, int population) {

	try {
		sql = connection.createStatement();
		rs = sql.executeQuery("select cid from country");

		//Check to see if the cid already exists
		while(rs.next()) { 
			int country_id = rs.getInt(1);
			if(country_id == cid)
				//Return false if the cid already exists
				return false; 
		}

		String insert = "insert into country values (" + cid + ", '" + name + "', " + height + ", " + population + ")";
		sql.executeUpdate(insert);

		//Return true if the new country was added
		return true; 
	    }
	
	catch(SQLException except) {
   		return false;
	}
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	try {
		sql = connection.createStatement();

		//Obtain the ocean ids and the number of countries which have a border with the ocean
		rs = sql.executeQuery("select oid, count(cid) from oceanAccess order by oid");

		while(rs.next()) {
			int ocean_id = rs.getInt(1);
			//return the number of countries if oid matches ocean_id
			if(ocean_id == oid) 
				return rs.getInt(2);
			
			}
		

		return 0;
		}
	catch(SQLException except) {
		return -1;  
	}  
}
   
  public String getOceanInfo(int oid){
	try {
		sql = connection.createStatement();

		// Obtain the oids for all oceans
          	rs = sql.executeQuery("select oid from ocean");
          
         	int loopCheck = 0;
          
          	while(rs.next()) {
              	int ocean_id = rs.getInt(1);

              	// if ocean exists, exit while loop
			if(ocean_id == oid) { 
                  	loopCheck = 1;
                  	break;
              }
          }
          
          	if(loopCheck == 0) {
			// return empty string if ocean does not exist 
              	return ""; 
          }
          
                
          // Acquire the tuple from ocean having ocean id as oid
          String ocean = "select oid, oname, depth from ocean where oid = " + oid;
          rs = sql.executeQuery(ocean);
          
          String returnValue = "";
          
          while(rs.next()) {
          		String ocean_name;
          		int ocean_id, ocean_depth;
          
          		// Store the ocean's information in 3 variables and return their concatenation
          		ocean_id = rs.getInt(1); 
          		ocean_name = rs.getString(2);
          		ocean_depth = rs.getInt(3);
          
    			returnValue += ocean_id + ":" + ocean_name + ":" + ocean_depth;
          }
          
          return returnValue;
      }

		catch(SQLException except) {
			return "";
		}
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	try {
		sql = connection.createStatement();

		//Obtain the cid and year for all hdi scores
		rs = sql.executeQuery("select cid, year from hdi");

		int loopCheck = 0;

		while(rs.next()) {
			int country_id = rs.getInt(1);
			int hdi_year = rs.getInt(2);

			// if entry exists, exit while loop
			if(country_id == cid && hdi_year == year) {
				loopCheck = 1;
				break;
			}
		}

		// return false if entry does not exist
		if(loopCheck == 0) {
   			return false;
		}

		// Update the hdi score and return true
		String updateHDI = "update hdi set hdi_score = " + newHDI + " where cid = " + cid + " and year = " + year;
		sql.executeUpdate(updateHDI);
		return true;
	}
      
      catch(SQLException except) {
          return false;
      }

  }

  public boolean deleteNeighbour(int c1id, int c2id){
	try {
		sql = connection.createStatement();

		//delete the entries where c1id is the country and c2id is the neighbor and vice versa; return true after deletion
		String update1 = "delete from neighbour where country = " + c1id + " and neighbor = " + c2id;	
		sql.executeUpdate(update1);
		String update2 = "delete from neighbour where country = " + c2id + " and neighbor = " + c1id;	
		sql.executeUpdate(update2);

		return true;
      }
      
      catch(SQLException except) {
          return false;
      }        
  }
  
  
  public String listCountryLanguages(int cid){
	try {
		sql = connection.createStatement();

		//Obtain the cid of all countries
		rs = sql.executeQuery("select cid from country");

		int loopCheck = 0;	

		while(rs.next()) {
			int country_cid = rs.getInt(1);

			//exit the while loop if country exists
			if(country_cid == cid) {
				loopCheck = 1;
				break;
			}
		}

		if(loopCheck == 0) {
			//return emptry string if country doesn't exist
			return "";
		}

		//spokenLanguages includes all languages for a country with the given cid; 
		String view = "create view temp as " + "select cid, lid, lname, lpercentage from language where cid = (select cid from country where cid = " + cid + ")";
          	sql.executeUpdate(view);

		String languages = "select lid, lname, (population * lpercentage) as population " +
					   "from country, spokenLanguages " +
					   "where spokenLanguages.cid = country.cid" + 
					   "order by population";

		//rs contains required info for all such countries
          	rs = sql.executeQuery(languages);
                   
         	String retValue = "";
          
         	while(rs.next()) {
             	int language_id = rs.getInt(1);
              	String language_name = rs.getString(2);
              	int population_count = rs.getInt(3);
              
              	retValue += language_id + ":" + language_name + ":" + population_count + "#";
          	}
         
		// drop the views created in this method before exiting          
         	sql.executeUpdate("drop view spokenLanguages"); 
         	sql.executeUpdate("drop view temp");
         
          	retValue = retValue.substring(0, retValue.length() - 1);
          	return retValue;
	}

     catch(SQLException except) {
     		return "";
     }

}
  
  public boolean updateHeight(int cid, int decrH){
	try {
		sql = connection.createStatement();

		//Obtain the cid of all countries
		rs = sql.executeQuery("select cid from country");

		int loopCheck = 0;	

		while(rs.next()) {
			int country_cid = rs.getInt(1);

			//exit the while loop if country exists
			if(country_cid == cid) {
				loopCheck = 1;
				break;
			}
		}

		if(loopCheck == 0) {
			//return false if country doesn't exist
			return false;
		}


          // Update the height of the country with the given cid; return true after update
          String updateVal = "update country " + 
					  "set height = height - " + decrH  + 
                             "where cid = " + cid + ")";

          sql.executeUpdate(updateVal);
          
          return true;
      }
      
      catch(SQLException except) {
          return false;
      }
  }
    
  public boolean updateDB(){
	try {
		sql = connection.createStatement();
            
		//population100 contains countries which have a population of over a 100 million             			
		sql.executeUpdate("create view population100 as " +
                             "select cid " +
                             "from country " +
                             "where population > 100000000");
            
		//create a table for the countries whose cid was in population100 
		sql.executeUpdate("create table mostPopulousCountries (" + "cid integer, cname varchar(20))");
            
		sql.executeUpdate("insert into mostPopulousCountries (" + 
					  "select cid, cname " +
                             "from country " +
                             "where cid in (select cid from population100) " +
                             "order by cid asc " +
                              ")");

           // drop the view before exiting
          	sql.executeUpdate("drop view population100 "); 
          
		return true;
        }
        
     	catch(SQLException except) {
          	return false;
     }
  }
  
}

