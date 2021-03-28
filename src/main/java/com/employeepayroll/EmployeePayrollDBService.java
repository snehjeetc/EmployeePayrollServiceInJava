package com.employeepayroll;

import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class EmployeePayrollDBService {
    private static String jdbcUrl = "jdbc:mysql://localhost:3306/payroll_service?useSSL=false";
    private static String userName = "root";
    private static String passWord = "Rooting@1";

    private Connection getConnection() throws SQLException {
        Connection connection;
        System.out.println("Connecting to database: " + jdbcUrl);
        connection = DriverManager.getConnection(jdbcUrl, userName, passWord);
        System.out.println("connection is successful!!!!" + connection);
        return connection;
    }

    public List<EmployeePayrollData> readData() {
        String selectQuery = "SELECT * from employee_payroll";
        List<EmployeePayrollData> employeePayrollList = new ArrayList<>();
        try(Connection connection = this.getConnection()){
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(selectQuery);
            while(resultSet.next()){
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                double salary = resultSet.getDouble("salary");
                LocalDate startDate = resultSet.getDate("start").toLocalDate();
                employeePayrollList.add(new EmployeePayrollData(id, name, salary, startDate));
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return employeePayrollList;
    }
}
