// A simple JDBC example.
// Remember that you need to put the jdbc postgresql driver in your class path
// when you run this code.
// See /local/packages/jdbc-postgresql on cdf for the driver, another example
// program, and a how-to file.

// To compile and run this program on cdf:
// (1) Compile
//     javac Example
// (2) Run
// Normally, you would run a Java program whose main method is in a class 
// called Example as follows:
//     java Example
// But we need to also give the class path to where JDBC is, so we type:
//     java -cp /local/packages/jdbc-postgresql/postgresql-8.4-701.jdbc4.jar: Example
// Alternatively, we can set our CLASSPATH variable in linux.  (See
// /local/packages/jdbc-postgresql/HelloPostgresql.txt on cdf for how.)

import java.sql.*;
import java.io.*;

public class Assignment2{
	Connection connection;
	Statement sql;
	PreparedStatement ps;
	ResultSet rs;

 	Assignment2(){
		try{
			Class.forName("org.postgresql.Driver");
        }
		catch (ClassNotFoundException e){
			System.out.println("Failed");
		}
	}

	public boolean connectDB(String URL, String username, String password){
		System.out.println(URL + " " + username + " " + password);
		try{	
			connection = DriverManager.getConnection(URL, username, password);
		}
		catch (SQLException se){
			//System.out.println("Failed.");
		}
		if(connection != null){
			//System.out.println("Connection Succeeded.");
			return true;
		}
		else{
			//System.out.println("Connection Failed.");
			return false;
		}
	} 
	
	  //Closes the connection. Returns true if closure was successful
	public boolean disconnectDB(){
        try{
			if(rs != null){
				rs.close();
			}
			if (ps != null){
				ps.close();
			}
			if (connection != null){
			    connection.close();
			}

            //System.out.println("Disconnection Succeeded");
        }
        catch (Exception e){
            System.out.println("Disconnection Failed");
            return false;
        }
        return true;
    }
	
	public boolean insertCountry (int cid, String name, int height, int population){
	    try{
			String query = "SET search_path TO A2;";
			ps = connection.prepareStatement(query);
			ps.execute();
			query = "SELECT cid from country where cid="+cid+";";
			ps = connection.prepareStatement(query);
			rs = ps.executeQuery();
			if (rs.next()){
				//System.out.println("cid exists");
				return false;
			}
			query = "INSERT INTO country (cid, cname, height, population) VALUES ("+cid+", '"+name+"', "+height+", "+population+");";
			ps = connection.prepareStatement(query);
			int check = ps.executeUpdate();
			//delete System.out.println(check);
			if (check == 1){
				return true;
			}
			else{
				return false;
			}
        }
        catch (SQLException se){
            System.err.println("SQL Exception."+"<Message>: "+se.getMessage());
            return false;
        }
    }
          
           
	public int getCountriesNextToOceanCount(int oid) {
		try{
			String query = "SET search_path TO A2;";
			ps = connection.prepareStatement(query);
			ps.execute();

			query = "SELECT count(cid) as cnum from oceanAccess where oid="+oid+";";
			ps = connection.prepareStatement(query);
			rs = ps.executeQuery();

			if (rs.next()){
				int answer = rs.getInt("cnum");
				return answer;
			}
			else{
				return -1;
			}
        }
        catch (SQLException se){
	    System.err.println("SQL Exception."+"<Message>: "+se.getMessage());
        return -1;
		}
	}

	   
	public String getOceanInfo(int oid){
		try{
			String query = "SET search_path TO A2;";
			ps = connection.prepareStatement(query);
			ps.execute();	  

			query = "SELECT * from ocean where oid="+oid+";";
			ps = connection.prepareStatement(query);
			rs = ps.executeQuery();

			if (rs.next()){
			String answer = rs.getInt("oid")+":"+rs.getString("oname")+":"+rs.getInt("depth");
            return answer;
			}
			else{
				String n = "";
				return n;
			}
		}
		catch (SQLException se){
			System.err.println("SQL Exception."+"<Message>: "+se.getMessage());
			String n = "";
			return n;
		}
	}


	public boolean chgHDI(int cid, int year, float newHDI){
		try{
			String query = "SET search_path to A2;";
			ps = connection.prepareStatement(query);
			ps.execute();          

			query = "UPDATE hdi set hdi_score="+newHDI+" where cid="+cid+" and year="+year+";";
			ps = connection.prepareStatement(query);
			int check = ps.executeUpdate();
			if (check == 1){
               return true;
			}
			else{
               return false;
			}
		}
		catch (SQLException se){
			System.err.println("SQL Exception."+"<Message>: "+se.getMessage());
			return false;
		}
	}

	  
	public boolean deleteNeighbour(int c1id, int c2id){
		try{
			String query = "SET search_path to A2;";
			ps = connection.prepareStatement(query);
			ps.execute();

			query = "DELETE from neighbour where country="+c1id+" and neighbor="+c2id+";";
			ps = connection.prepareStatement(query);
			int check1 = ps.executeUpdate();
	  
			query = "DELETE from neighbour where country="+c2id+" and neighbor="+c1id+";";
			ps = connection.prepareStatement(query);
			int check2 = ps.executeUpdate();
	  
			if (check1==1 && check2==1){
				return true;
			}  
			else{
				return false;
			}        
		}
		catch (SQLException se){
			System.err.println("SQL Exception."+"<Message>: "+se.getMessage());
			return false;
		}
	}

	  
	public String listCountryLanguages(int cid){
		try{
			String query = "SET search_path to A2;";
			ps = connection.prepareStatement(query);
			ps.execute();

			query = "SELECT l.lid, l.lname, (c.population*l.lpercentage) as lpopulation from language as l, country as c where l.cid="+cid+" and l.cid=c.cid;";
			ps = connection.prepareStatement(query);
			rs = ps.executeQuery();

			String result = "";
			while (rs.next()){
				result = result+rs.getInt("lid")+":"+rs.getString("lname")+":"+rs.getFloat("lpopulation")+"#";
		}
			if (result!=""){
				return result.substring(0,result.length()-1);
			}
			else{
				return result;
			}
		}
		catch (SQLException se){
			System.err.println("SQL Exception."+"<Message>: "+se.getMessage());
			String wrong = "";
			return wrong;
		}
	}


	public boolean updateHeight(int cid, int decrH){
		try{
			String query = "SET search_path to A2;";
			ps = connection.prepareStatement(query);
			ps.execute();

			query = "SELECT height from country where cid="+cid+";";
			ps = connection.prepareStatement(query);
			rs = ps.executeQuery();

			int oldheight;
			if (rs.next()){
				oldheight = rs.getInt("height");
			}
			else{
				return false;
			}

			query = "UPDATE country set height="+(oldheight-decrH)+" where cid="+cid+";";
			ps = connection.prepareStatement(query);
			int check = ps.executeUpdate();

			if (check == 1){
				return true;
			}
			else {
                return false;
			}
		}
		catch (SQLException se)
		{
			System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
			return false;
		}
	}


	public boolean updateDB(){
		try{
			String query = "SET search_path to A2;";
			ps = connection.prepareStatement(query);
			ps.execute();

			query = "CREATE TABLE mostPopulousCountries(cid int, cname varchar(20));";
			ps = connection.prepareStatement(query);
			int check = ps.executeUpdate();
			//System.out.println(check);
			if (check!=0){
				return false;
			}
			query = "SELECT cid, cname from country where population>100000000 order by cid ASC;";
			ps = connection.prepareStatement(query);
			rs = ps.executeQuery();

			if (rs == null){
				return false;
			}
			while (rs.next()){
				query = "INSERT INTO mostPopulousCountries (cid, cname) VALUES ("+rs.getInt("cid")+", '"+rs.getString("cname")+"');";
				ps = connection.prepareStatement(query);
				check = ps.executeUpdate();
				if (check!=1){
					return false;
				}
			}
			return true;
		}
		catch (SQLException se){
			System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
			return false;
		}
	}

	/******************************************************************************/	  
    /* Following main function is used to test and demo the results*/
	/******************************************************************************/	
    /*
    public static void main(String args[]) throws IOException
        {
		Assignment2 demo = new Assignment2();
                String userid = "c4leejoh";
		String password = "";
                String url = "jdbc:postgresql://localhost:5432/csc343h-"+userid;
		demo.connectDB(url, userid, password);
                
                System.out.println(demo.insertCountry(17,"test",10,100));
                System.out.println(demo.getCountriesNextToOceanCount(1));
                System.out.println(demo.getOceanInfo(1));
                System.out.println(demo.chgHDI(1,2010, 0.9f));
                System.out.println(demo.deleteNeighbour(1,5));
                System.out.println(demo.listCountryLanguages(2));
                System.out.println(demo.updateHeight(1,1));
                System.out.println(demo.updateDB());
                demo.disconnectDB();
        }
	*/
	/******************************************************************************/
}
