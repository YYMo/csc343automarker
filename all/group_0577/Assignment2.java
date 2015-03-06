import java.sql.*;
import java.io.*;

public class Assignment2 {
	
	// a connection to the database 
	Connection connection;
	
	PreparedStatement ps = null;
	
	ResultSet rs;
	
	Assignment2(){
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {

        }
    }
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	public boolean connectDB(String URL, String username, String password){
		
		try {
			
			// making the connection to the database
			connection = DriverManager.getConnection(URL, username, password);
			
		} catch (SQLException e){
			
			//System.out.println("The connection cannot  be completed!");
			e.printStackTrace();
			return false;
		}
		
		if (connection != null){
			
			//System.out.println("connection successful");
			return true;
		}
		
		else {
			//System.out.println("connection failed!");
			return false;
		}
		
	}
	
	public boolean disconnectDB(){
		
		if (connection != null){
			
			try {
				connection.close();
				ps.close();
				rs.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}
			return true;
		}
		
		else {

			return false;
		}
		
	}
	
	public boolean insertCountry(int cid, String name, int height, int population){
		
		
		
		boolean result = false;
		String sql = "INSERT INTO a2.country(cid, cname, height, population)" + "VALUES(?, ?, ?, ?)";
		//String check = "SELECT * FROM a2.country WHERE cid = ?";
		
		try{
			
//			ps = connection.prepareStatement(check);
//			ps.setInt(1, cid);
//			rs = ps.executeQuery();
//					if(rs.next()){
//						System.out.println("country with the same cid already exist!");
//					} else {
			
			ps = connection.prepareStatement(sql);
			ps.setInt(1, cid);
			ps.setString(2, name);
			ps.setInt(3, height);
			ps.setInt(4, population);
			//ps.executeQuery();
			
			//checking
			int effected = ps.executeUpdate();
			if (effected == 1){
				result = true;
			}
			else{
				return false;
			}
			
			
			
		} catch (Exception e) {
			//e.printStackTrace();
		}
		
		return result;
		
	}
	
	public int getCountriesNextToOceanCount(int oid){
		
		int ret = -1;
		int count = 0;
		String query = "SELECT * FROM a2.oceanAccess WHERE oid = ?";
		
		try{
			ps = connection.prepareStatement(query);
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			while (rs.next()){
				++count;
			}
			
			return count;
		} catch (Exception e){
			//e.printStackTrace();
			return ret;
		}
				
	}
	
	public String getOceanInfo(int oid){
		
		String ret = "";
		String query = "SELECT * FROM a2.ocean WHERE oid = ?";
		
		try{
			ps = connection.prepareStatement(query);
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			if (rs.next()){
				int theOid = rs.getInt("oid");
				String theOname = rs.getString("oname");
				int theDepth = rs.getInt("depth");
				ret = theOid + ":" + theOname + ":" + theDepth;
			}else{
				return ret;
			}
			
		} catch (Exception e){
			//e.printStackTrace();
		}
		
		return ret;
	}
	
	public boolean chgHDI(int cid, int year, float newHDI){
		
		boolean ret = false;
		
		String query = "UPDATE a2.hdi SET hdi_score = ? where cid = ? AND year = ?";
		//String check = "SELECT * FROM a2.hdi WHERE hdi_score = ? AND year = ?";
		
		try{
			ps = connection.prepareStatement(query);
			ps.setFloat(1, newHDI);
			ps.setInt(2, cid);
			ps.setInt(3, year);
			int effected = ps.executeUpdate();
			
			
			if (effected == 1){
				ret = true;
			}else{
				//System.out.println("update not successful!");
				return false;
			}
			
		} catch (Exception e){
			//e.printStackTrace();
		}
		return ret;
	}
	
	public boolean deleteNeighbour(int c1id, int c2id){
		
		boolean ret = false;
		String query = "DELETE FROM a2.neighbour WHERE country = ? and neighbor = ?"; // IS THIS VALID???
		String check = "SELECT * FROM a2.neighbour WHERE country = ? and neighbor = ?";
		
		try{
			ps = connection.prepareStatement(query);
			ps.setInt(1, c1id);
			ps.setInt(2, c2id);
			ps.executeUpdate();
			
			ps.close();
			ps = null;
			
			//swap
			
			ps = connection.prepareStatement(query);
			ps.setInt(1, c2id);
			ps.setInt(2, c1id);
			ps.executeUpdate();
			
			ret = true;
			
			
			
			} catch (Exception e){
				//e.printStackTrace();
			}
			
		
		return ret;
		
	}
	
	public String listCountryLanguages(int cid){
		
		String result = "";
		String query = "SELECT lid, cname as lname, (lpercentage * population) as population FROM a2.country NATURAL JOIN a2.language WHERE cid = ? ORDER BY population";
		
		try{
			
				ps = connection.prepareStatement(query);
				ps.setInt(1, cid);
				rs = ps.executeQuery();
				
				while (rs.next()){
				
					int theLid = rs.getInt("lid");
					String theName = rs.getString("lname");
					int thePopulation = rs.getInt("population");
					
					result += theLid + ":" + theName + ":" + thePopulation + "#";
					
					
				}
			
			
		}catch (Exception e){
			//e.printStackTrace();
		}
		
		int length = (result.length()) - 1;
		
		return result.substring(0, length);
	}
	
	public boolean updateHeight(int cid, int decrH){
		
		boolean ret = false;
		
		String query = "SELECT * FROM a2.country WHERE cid = ?";
		
		try{
			ps = connection.prepareStatement(query);
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			while (rs.next()){
				int theHeight = rs.getInt("height");
				theHeight = theHeight - decrH;
				if (theHeight < 0){
					return false;
				}
				rs.updateInt("height", theHeight);
				rs.updateRow();
			}
			ret = true;
			
			
			
		} catch (Exception e){
			//e.printStackTrace();
		}
		return ret;
	
	}
	
	public boolean updateDB(){
		boolean ret = false;
		String drop = "DROP TABLE IF EXISTS a2.mostPopulousCountries"; //shud i put a2. on it???
		String create = "CREATE TABLE a2.mostPopulousCountries(cid INTEGER, cname VARCHAR(20))";
		String query = "SELECT cid, population FROM a2.country WHERE population > 100000000 ORDER BY cid DESC";
		
		try{
			ps = connection.prepareStatement(drop);
			ps.executeUpdate();
			ps = connection.prepareStatement(create);
			ps.executeUpdate();
			ps = connection.prepareStatement(query);
			rs = ps.executeQuery();
			while (rs.next()){
				int theCid = rs.getInt("cid");
				String theName = rs.getString("cname");
				String insert = "INSERT INTO mostPopulousCountries VALUES (?, ?)";
				ps = connection.prepareStatement(insert);
				ps.setInt(1, theCid);
				ps.setString(2, theName);
				ps.executeUpdate();
			}
			ret = true;
			
		}catch (Exception e){
			//e.printStackTrace();
		}
		return ret;
	}
	
	

}
