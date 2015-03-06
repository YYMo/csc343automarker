import java.sql.*;

//TODO delete it
import java.io.*;

public class Assignment2 {

    // A connection to the database
    Connection connection;
    // Statement to run queries
    Statement sql;
    // Prepared Statement
    PreparedStatement ps;
	PreparedStatement ps2;
    // Resultset for the query
    ResultSet rs;
    String countryLanguages ;
    //CONSTRUCTOR
    Assignment2(){
        try {
            Class.forName("org.postgresql.Driver");
        }
        catch (ClassNotFoundException e) {
            System.out.println("Failed to find the JDBC driver");
            e.printStackTrace();
            return;
        }
    }

    //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password){
        try {
            connection = DriverManager.getConnection(URL, username, password);
            return true;
        } catch (SQLException e) {
            System.out.println("Connection Failed");
            e.printStackTrace();
            return false;
        }

    }

    //Closes the connection. Returns true if closure was sucessful
    public boolean disconnectDB(){
        try {
            connection.close();
            return true;
        } catch (SQLException e) {
            System.out.println("Disconnection Failed");
            e.printStackTrace();
            return false;
        }
    }

    public boolean insertCountry (int cid, String name, int height, int population) {
        try{

            String sqlText;
            sqlText = "Select cid FROM a2.country WHERE cid = ?";
            ps = connection.prepareStatement(sqlText);
            ps.setInt(1,cid);
            rs = ps.executeQuery();

            if (rs.next()){
                rs.close();
                ps.close();
                return false;
            }
            else{
                sqlText = "INSERT INTO a2.country " +
                             "VALUES (?,?,?,?) ";
                //ps.close();
                ps = connection.prepareStatement(sqlText);
                ps.setInt(1,cid);
                ps.setString(2,name);
                ps.setInt(3,height);
                ps.setInt(4,population);
                ps.executeUpdate();
                ps.close();

                rs.close();
                return true;
            }
        }catch (SQLException e) {
            System.out.println("Query execution failed!");
            e.printStackTrace();
            return false;
        }

    }





    public int getCountriesNextToOceanCount(int oid) {
        try{
            String sqlText;
            sqlText = "SELECT COUNT(cid) as number FROM a2.oceanAccess WHERE oid = ? GROUP BY oid";
            ps = connection.prepareStatement(sqlText);

            ps.setInt(1, oid);
            rs = ps.executeQuery();
            int numberofCountry;
            if (rs.next()){
                numberofCountry = rs.getInt("number");

            }
            else{
                numberofCountry = 0;
            }
            rs.close();
            ps.close();
            return numberofCountry;
        }catch (SQLException e) {
            System.out.println("Query execution failed!");
            e.printStackTrace();
            return -1;
        }
    }



    public String getOceanInfo(int oid){
       try{
            String sqlText;
            sqlText = "SELECT * FROM a2.ocean WHERE oid = ? ";
            ps = connection.prepareStatement(sqlText);

            ps.setInt(1, oid);

            rs = ps.executeQuery();
            String OceanInfo;

            if (rs.next()){
                int _oid = rs.getInt("oid");
                String oname = rs.getString("oname");
                int depth = rs.getInt("depth");
                OceanInfo = String.valueOf(_oid)+":"+ oname+":"+String.valueOf(depth);

            }
            else{
                OceanInfo = "";
            }
           // rs.close();
            ps.close();
            return OceanInfo;
        }catch (SQLException e) {
            System.out.println("Query execution failed!");
            e.printStackTrace();
            return null;
        }
    }



    public boolean chgHDI(int cid, int year, float newHDI){
       try{
            String sqlText;
            sqlText = "SELECT * FROM a2.hdi WHERE cid =?  AND year = ? ";
            ps = connection.prepareStatement(sqlText);
            ps.setInt(1,cid);
            ps.setInt(2,year);
            rs = ps.executeQuery();

            if (!rs.next()){
            rs.close();
            ps.close();
            return false;

            }
            else{

                sqlText = "UPDATE a2.hdi SET  hdi_score = ? WHERE cid = ? AND year = ?";
                ps.close();
                ps = connection.prepareStatement(sqlText);

                ps.setFloat(1,newHDI);
                ps.setInt(2,cid);
                ps.setInt(3,year);
                ps.executeUpdate();
                //ps.executeQuery
                ps.close();

                rs.close();
                return true;
            }
        }catch (SQLException e) {
            System.out.println("Query execution failed!");
            e.printStackTrace();
            return false;
        }
    }



    public boolean deleteNeighbour(int c1id, int c2id){
        try{

            String sqlText;

            sqlText = "DELETE FROM a2.neighbour WHERE country = ? AND neighbor = ? ";
            ps = connection.prepareStatement(sqlText);
            ps.setInt(1,c1id);
            ps.setInt(2,c2id);
            ps.executeUpdate();
            ps.setInt(1,c2id);
            ps.setInt(2,c1id);
            ps.executeUpdate();

            ps.close();
            return true;

        }catch (SQLException e) {
                System.out.println("deleteNeighbour Failed");
                e.printStackTrace();
                return false;
        }
    }





    public String listCountryLanguages(int cid){
           try{
            String sqlText;
            sqlText = "SELECT language.lid, country.cname, (language.lpercentage + country.population) as  population FROM a2.language, a2.country WHERE language.cid = country.cid AND country.cid = ? ORDER BY population;";
            ps = connection.prepareStatement(sqlText);

            countryLanguages = "";
            ps.setInt(1, cid);
            rs = ps.executeQuery();


            if (rs != null){
                int i = 1;

                while (rs.next()) {
                    int lid = rs.getInt("lid");

                    String lname = rs.getString("cname");
                    Float population = rs.getFloat("population");

                    countryLanguages += "l"+ String.valueOf(i) + String.valueOf(lid)+ ":" +
                                                "l"+ String.valueOf(i) + lname+ ":" +
                                                "l"+ String.valueOf(i) + String.valueOf(population) + "#";
                    i++;
                }

            }
            else{
                countryLanguages = "";
            }
            rs.close();
            ps.close();

            return countryLanguages;

        }catch (SQLException e) {
            System.out.println("listCountryLanguages Failed");
            e.printStackTrace();
            return null;
        }
    }


    public boolean updateHeight(int cid, int decrH){
        try{
            String sqlText;
            sqlText = "Select * FROM a2.country  WHERE cid =?";
            ps = connection.prepareStatement(sqlText);
            ps.setInt(1, cid);
            rs = ps.executeQuery();

            if (!rs.next()){
                rs.close();
                ps.close();
                return false;
            }
            else{

                sqlText = "UPDATE a2.country " +
                             "SET height = height - ?" +
                             "WHERE cid = ?";
                ps.close();
                ps = connection.prepareStatement(sqlText);
                ps.setInt(1,decrH);
                ps.setInt(2,cid);
                ps.executeUpdate();
                ps.close();

                rs.close();
                return true;
            }
        }catch (SQLException e) {
            System.out.println("Query execution failed!");
            e.printStackTrace();
            return false;
        }
    }




    public boolean updateDB(){
        try{
            sql = connection.createStatement();
            String sqlText;
            sqlText = "DROP TABLE IF EXISTS a2.mostPopulousCountries;" +
                        "CREATE TABLE a2.mostPopulousCountries(" +
                        "                  cid INTEGER REFERENCES a2.country(cid) ON DELETE RESTRICT," +
                        "                  cname VARCHAR(20) , PRIMARY KEY(cid))";

            sql.executeUpdate(sqlText);

            sqlText = "Select cid, cname FROM a2.country  WHERE population > 100000000 ORDER BY cid ASC ";
            ps = connection.prepareStatement(sqlText);
            rs = ps.executeQuery();

             if (rs!=null){

                sqlText = "INSERT INTO a2.mostPopulousCountries VALUES (?,?)";
                ps2 = connection.prepareStatement(sqlText);

                int i = 1;
                while (rs.next()) {
                    int cid = rs.getInt("cid");
                    String name = rs.getString("cname");
                    ps2.setInt(1,cid);
                    ps2.setString(2,name);
					ps2.executeUpdate();

                    i++;
                }
            }
            ps.close();
			ps2.close();
            sql.close();
            rs.close();
            return true;

        }catch (SQLException e) {
            System.out.println("Query execution failed!");
            e.printStackTrace();
            return false;
        }

    }



}
