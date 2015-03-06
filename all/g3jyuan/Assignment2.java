import java.sql.*;

public class Assignment2 {

    private Connection connection;

    Assignment2() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("Failed to load postgresql driver.");
        }
    }

    public boolean connectDB(String URL, String username, String password) {
        try {
            this.connection = DriverManager.getConnection(URL, username, password);
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    public boolean disconnectDB() {
        try {
            this.connection.close();
            return this.connection.isClosed();
        } catch (SQLException e) {
            return false;
        } catch (NullPointerException e) {
            return false;
        }
    }

    public boolean insertCountry(int cid, String name, int height, int population) {
        try {
            String q = "INSERT INTO a2.country VALUES (?, ?, ?, ?);";
            PreparedStatement ps = this.connection.prepareStatement(q);
            ps.setInt(1, cid);
            ps.setString(2, name);
            ps.setInt(3, height);
            ps.setInt(4, population);

            int numInserted = ps.executeUpdate();
            ps.close();

            return numInserted == 1;
        } catch (SQLException e) {
            return false;
        }
    }

    public int getCountriesNextToOceanCount(int oid) {
        try {
            String q = "SELECT COUNT(*) FROM a2.oceanAccess WHERE oid = ?";
            PreparedStatement ps = this.connection.prepareStatement(q);
            ps.setInt(1, oid);

            ResultSet rs = ps.executeQuery();
            rs.next();
            int num = rs.getInt(1);
            rs.close();
            ps.close();

            return num;
        } catch (SQLException e) {
            return -1;
        }
    }

    public String getOceanInfo(int oid) {
        try {
            String q = "SELECT * FROM a2.ocean WHERE oid = ?";
            PreparedStatement ps = this.connection.prepareStatement(q);
            ps.setInt(1, oid);

            ResultSet rs = ps.executeQuery();
            rs.next();
            int oid_result = rs.getInt(1);
            String oname = rs.getString(2);
            int depth = rs.getInt(3);
            rs.close();
            ps.close();

            return String.format("%d:%s:%d", oid_result, oname, depth);
        } catch (SQLException e) {
            return "";
        }
    }

    public boolean chgHDI(int cid, int year, float newHDI) {
        try {
            String q = "UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? AND year = ?";
            PreparedStatement ps = this.connection.prepareStatement(q);
            ps.setFloat(1, newHDI);
            ps.setInt(2, cid);
            ps.setInt(3, year);

            int numUpdated = ps.executeUpdate();
            ps.close();

            return numUpdated == 1;
        } catch (SQLException e) {
            return false;
        }
    }

    public boolean deleteNeighbour(int c1id, int c2id) {
        try {
            String q = "DELETE FROM a2.neighbour WHERE country = ? AND neighbor = ?";
            PreparedStatement ps = this.connection.prepareStatement(q);
            ps.setInt(1, c1id);
            ps.setInt(2, c2id);

            int numUpdated = ps.executeUpdate();
            ps.setInt(1, c2id);
            ps.setInt(2, c1id);
            numUpdated += ps.executeUpdate();
            ps.close();

            return numUpdated == 2;
        } catch (SQLException e) {
            return false;
        }
    }

    public String listCountryLanguages(int cid) {
        try {
            String q = "SELECT lid, lname, (lpercentage * population) AS population " +
                       "FROM a2.country JOIN a2.language USING (cid) " +
                       "WHERE cid = ? ORDER BY population";
            PreparedStatement ps = this.connection.prepareStatement(q);
            ps.setInt(1, cid);

            ResultSet rs = ps.executeQuery();
            rs.next();
            StringBuilder sb = new StringBuilder();
            while (!rs.isAfterLast()) {
                sb.append(String.format("%d:%s:%s#", rs.getInt(1), rs.getString(2), rs.getString(3)));
                rs.next();
            }
            rs.close();
            ps.close();

            return sb.length() > 0 ? sb.substring(0, sb.length() - 1) : "";
        } catch (SQLException e) {
            return "";
        }
    }

    public boolean updateHeight(int cid, int decrH) {
        try {
            String q = "UPDATE a2.country SET height = height - ? WHERE cid = ?";
            PreparedStatement ps = this.connection.prepareStatement(q);
            ps.setInt(1, decrH);
            ps.setInt(2, cid);

            int numUpdated = ps.executeUpdate();
            ps.close();

            return numUpdated == 1;
        } catch (SQLException e) {
            return false;
        }
    }

    public boolean updateDB() {
        try {
            String q = "CREATE TABLE a2.mostPopulousCountries " +
                       "(cid INTEGER, cname VARCHAR(20))";
            PreparedStatement ps = this.connection.prepareStatement(q);

            ps.executeUpdate();
            ps.close();

            q = "INSERT INTO a2.mostPopulousCountries " +
                "SELECT cid, cname FROM a2.country WHERE population > 100000000 " +
                "ORDER BY cid";
            ps = this.connection.prepareStatement(q);

            ps.executeUpdate();
            ps.close();

            return true;
        } catch (SQLException e) {
            return false;
        }
    }
}
