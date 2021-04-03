package com.employeepayroll;

import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EmployeePayrollDBService {
    private static String jdbcUrl = "jdbc:mysql://localhost:3306/employeePayrollDB?useSSL=false";
    private static String userName = "root";
    private static String passWord = "Rooting@1";

    private static EmployeePayrollDBService employeePayrollDBService;
    private PreparedStatement employeePayrollDataStatement;

    private EmployeePayrollDBService(){

    }

    public static EmployeePayrollDBService getInstance(){
        if(employeePayrollDBService == null)
            employeePayrollDBService = new EmployeePayrollDBService();
        return employeePayrollDBService;
    }

    int connectionID = 0;

    private Connection getConnection() throws SQLException {
        Connection connection;

        System.out.println("Connecting to database: " + jdbcUrl);
        connection = DriverManager.getConnection(jdbcUrl, userName, passWord);
        System.out.println("connection is successful!!!!" + connection);
        return connection;
    }

    public List<EmployeePayrollData> readData() {
        String selectQuery = "SELECT * from employee_payroll";
        return this.getEmployeePayrollDataForGivenSql(selectQuery);
    }

    public int updateEmployeeData(String name, double salary) {
        String sqlUpdate = String.format("UPDATE employee_payroll SET salary = %.2f WHERE name = '%s';", salary, name);
        try(Connection connection = this.getConnection()){
            Statement statement = connection.createStatement();
            return statement.executeUpdate(sqlUpdate);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return 0;
    }

    public List<EmployeePayrollData> getEmployeePayrollData(String name) {
        List<EmployeePayrollData> employeePayrollList = null;
        if(this.employeePayrollDataStatement == null)
            this.prepareStatementForEmployeeData();
        try{
            employeePayrollDataStatement.setString(1, name);
            ResultSet resultSet = employeePayrollDataStatement.executeQuery();
            employeePayrollList = this.getEmployeePayrollData(resultSet);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return employeePayrollList;
    }

    private List<EmployeePayrollData> getEmployeePayrollData(ResultSet resultSet){
        List<EmployeePayrollData> employeePayrollDataList = new ArrayList<>();
        try {
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                double salary = resultSet.getDouble("salary");
                LocalDate startDate = resultSet.getDate("start").toLocalDate();
                employeePayrollDataList.add(new EmployeePayrollData(id, name, salary, startDate));
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return employeePayrollDataList;
    }

    private List<EmployeePayrollData> getEmployeePayrollDataForGivenSql(String sql){
        try(Connection connection = this.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            return getEmployeePayrollData(resultSet);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return null;
    }

    public void prepareStatementForEmployeeData(){
        try{
            Connection connection = this.getConnection();
            String sql = "SELECT * FROM employee_payroll WHERE name = ?";
            employeePayrollDataStatement = connection.prepareStatement(sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public int updateEmployeeDataPreparedStatement(String name, double salary) {
        try(Connection connection = this.getConnection()){
            String sql = "UPDATE employee_payroll SET salary = ? WHERE name = ? ";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setDouble(1, salary);
            preparedStatement.setString(2, name);
            int resultSet = preparedStatement.executeUpdate();
            return resultSet;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return 0;
    }

    public EmployeePayrollData addEmployeeToPayrollUC7(String name, double salary, LocalDate startDate, String gender) {
        int employeeId = -1;
        EmployeePayrollData employeePayrollData = null;
        String sql = String.format("INSERT INTO employee_payroll (name, gender, salary, start)VALUES " +
                "('%s', '%s', %s, '%s')", name, gender, salary, Date.valueOf(startDate));
        try(Connection connection = this.getConnection()){
            Statement statement = connection.createStatement();
            int rowAffected = statement.executeUpdate(sql, statement.RETURN_GENERATED_KEYS);
            if(rowAffected == 1){
                ResultSet resultSet = statement.getGeneratedKeys();
                if(resultSet.next()) employeeId = resultSet.getInt(1);
            }
            employeePayrollData = new EmployeePayrollData(employeeId, name, salary, startDate);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return employeePayrollData;
    }

    public List<EmployeePayrollData> getEmployeePayrollDataBetweenDates(String from, String to) {
        //if to is null, then the end date will be the present date

        Date start = Date.valueOf(from);
        Date end = (to == null) ? Date.valueOf(LocalDate.now()) : Date.valueOf(to);
        String sql = String.format("SELECT * FROM employee_payroll WHERE start BETWEEN '%s' AND '%s'",
                start, end);
        return this.getEmployeePayrollDataForGivenSql(sql);

        //The prepare statement is costly here, as we are closing the connection after
        //executing the whole try block
        //The best way would be to use Statement instead of preparedStatement
    }

    public List<String> calculateSumAverageMinMax() {
        List<String> outputFromDB = new ArrayList<>();
        try(Connection connection = this.getConnection()){
            String sql = "SELECT SUM(salary), AVG(salary), MIN(salary), MAX(salary) FROM employee_payroll";
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while(resultSet.next()) {
                outputFromDB.add(Double.toString(resultSet.getDouble("SUM(salary)")));
                outputFromDB.add(Double.toString(resultSet.getDouble("AVG(salary)")));
                outputFromDB.add(Double.toString(resultSet.getDouble("MIN(salary)")));
                outputFromDB.add(Double.toString(resultSet.getDouble("MAX(salary)")));
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return outputFromDB;
    }


    public Map<String, List<Double>> calculateSumAverageMinMax_GroupByGender() {
        Map<String, List<Double>> outputMap = new HashMap<>();
        try(Connection connection = this.getConnection()) {
            String sql = "SELECT gender, SUM(salary), AVG(salary), MIN(salary), MAX(salary) " +
                    "FROM employee_payroll GROUP BY gender";
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while(resultSet.next()){
                String gender = resultSet.getString("gender");
                List<Double> fieldList = new ArrayList<>();
                fieldList.add(resultSet.getDouble("SUM(salary)"));
                fieldList.add(resultSet.getDouble("AVG(salary)"));
                fieldList.add(resultSet.getDouble("MIN(salary)"));
                fieldList.add(resultSet.getDouble("MAX(salary)"));
                outputMap.put(gender, fieldList);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return outputMap;
    }

    public EmployeePayrollData addEmployeeToPayroll(String name, double salary, LocalDate startDate, String gender) {
        int employeeId = -1;
        Connection connection = null;
        EmployeePayrollData employeePayrollData = null;
        try{
            connection =  this.getConnection();
            connection.setAutoCommit(false);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        try(Statement statement = connection.createStatement()){
            String sql = String.format("INSERT INTO employee_payroll (name, gender, salary, start)VALUES " +
                                     "('%s', '%s', %s, '%s')", name, gender, salary, Date.valueOf(startDate));
            int rowAffected = statement.executeUpdate(sql, statement.RETURN_GENERATED_KEYS);
            if(rowAffected == 1){
                ResultSet resultSet = statement.getGeneratedKeys();
                if(resultSet.next()) employeeId = resultSet.getInt(1);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        try(Statement statement = connection.createStatement()){
            double deductions = salary * 0.2;
            double taxablePay = salary - deductions;
            double tax = taxablePay * 0.1;
            double netPay = salary - tax;
            String sql = String.format("INSERT INTO payroll_details " +
                                     "(employee_id, basic_pay, deductions, taxable_pay, tax, net_pay) VALUES" +
                                     "(%s, %s, %s, %s, %s, %s);",
                                      employeeId, salary, deductions, taxablePay, tax, netPay);
            int rowaffected = statement.executeUpdate(sql);
            if(rowaffected == 1)
                employeePayrollData = new EmployeePayrollData(employeeId, name, salary, startDate);
            /*
            if the above try catch that is if the employee_payroll table gets updated successfully
            but there is some problem arises and the updation fails in this try block that is
            in payroll_details, then the updation will not be in sync and hence in one table updation
            will be visible while in other it will not (that is linkage will not happen)
            To avoid this we have to Transaction principle
            (to do this we do autoCommit(false) and commit() at the end of the execution if it is success.
            */
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        try {
            connection.commit();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            return employeePayrollData;
        }finally{
            if(connection != null) {
                try {
                    connection.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
        return employeePayrollData;
    }

    //MultiThreading addition of one employee
    public EmployeePayrollData addEmployeeToPayroll_MulitThreadingConcept(String name, double salary,
                                                                          LocalDate startDate,
                                                                          String gender){
        Integer employeeId[] = new Integer[] {-1};
        Connection connection = null;
        EmployeePayrollData employeePayrollData = null;
        try{
            connection =  this.getConnection();
            connection.setAutoCommit(false);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        Boolean[] isFirstProcessDone = new Boolean[] {false};
        Connection finalConnection = connection;
        Runnable task1 = () -> {
            System.out.println("Employee Being added: "+Thread.currentThread().getName());
            employeeId[0] = this.addEmployeeToPayroll(finalConnection, name, salary, startDate, gender);
            isFirstProcessDone[0] = true;
        };

        Integer[] rowAffected = new Integer[] {0};
        Runnable task2 = () -> {
            System.out.println("Added in employee payroll: "+Thread.currentThread().getName());
            rowAffected[0] = this.addToPayroll(finalConnection, employeeId[0], salary);
        };

        Thread thread1 = new Thread(task1, "Employee payroll thread: " + name);
        Thread thread2 = new Thread(task2, "Payroll thread: " + name);
        thread1.start();
        while(!isFirstProcessDone[0]) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if(employeeId[0] == -1)
            return null;
        thread2.start();
        return new EmployeePayrollData(employeeId[0], name, salary, startDate);
    }

    private int addEmployeeToPayroll(Connection connection, String name, double salary, LocalDate startDate, String gender) {
        int employeeId = -1;
        try(Statement statement = connection.createStatement()){
            String sql = String.format("INSERT INTO employee_payroll (name, gender, salary, start)VALUES " +
                    "('%s', '%s', %s, '%s')", name, gender, salary, Date.valueOf(startDate));
            int rowAffected = statement.executeUpdate(sql, statement.RETURN_GENERATED_KEYS);
            if(rowAffected == 1){
                ResultSet resultSet = statement.getGeneratedKeys();
                if(resultSet.next()) employeeId = resultSet.getInt(1);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return employeeId;
    }

    private int addToPayroll(Connection connection, Integer employeeId, double salary){
        int rowAffected = 0;
        try(Statement statement = connection.createStatement()){
            double deductions = salary * 0.2;
            double taxablePay = salary - deductions;
            double tax = taxablePay * 0.1;
            double netPay = salary - tax;
            String sql = String.format("INSERT INTO payroll_details " +
                            "(employee_id, basic_pay, deductions, taxable_pay, tax, net_pay) VALUES" +
                            "(%s, %s, %s, %s, %s, %s);",
                    employeeId, salary, deductions, taxablePay, tax, netPay);
            rowAffected = statement.executeUpdate(sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        try {
            connection.commit();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }finally{
            if(connection != null) {
                try {
                    connection.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
        return rowAffected;
    }
}
