package com.employeepayroll.ermodel;

import com.employeepayroll.EmployeePayrollData;

import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ERModelDBService {
        private static String jdbcUrl = "jdbc:mysql://localhost:3306/employeePayrollDB?useSSL=false";
        private static String userName = "root";
        private static String passWord = "Rooting@1";
        private static ERModelDBService erModelDBService;
        private static PreparedStatement preparedStatement;

        private ERModelDBService(){
        }

        public static ERModelDBService getInstance(){
            if(erModelDBService == null)
                erModelDBService = new ERModelDBService();
            return erModelDBService;
        }

        private Connection getConnection() throws SQLException {
            Connection connection;
            System.out.println("Connecting to database: " + jdbcUrl);
            connection = DriverManager.getConnection(jdbcUrl, userName, passWord);
            System.out.println("connection is successful!!!!" + connection);
            return connection;
        }

        private void prepareStatement() throws ERModelExceptions {
            try{
                Connection connection = this.getConnection();
                String sql = "SELECT e.emp_id, e.name, e.start, p.basic_pay " +
                             "FROM employee e " +
                             "JOIN payroll p ON e.emp_id = p.emp_id " +
                             "WHERE e.name = ?;";
                preparedStatement = connection.prepareStatement(sql);
            } catch (SQLException e) {
               throw new ERModelExceptions(ERModelExceptions.Status.CONNECTION_FAILURE);
            }
        }

        public List<EmployeePayrollData> readData(Map<Integer, Department> departmentMap) throws ERModelExceptions {
            List<EmployeePayrollData> employeePayrollDataList = new ArrayList<>();
            Connection connection = null;
            try{
                connection = getConnection();
            } catch (SQLException e) {
                throw new ERModelExceptions(ERModelExceptions.Status.CONNECTION_FAILURE);
            }
            try(Statement statement = connection.createStatement()) {
                String sql = "SELECT * FROM department";
                ResultSet resultSet = statement.executeQuery(sql);
                while(resultSet.next()){
                    Integer department_id = resultSet.getInt("department_id");
                    String department_name = resultSet.getString("department_name");
                    Department department = new Department(department_id, department_name);
                    departmentMap.put(department_id, department);
                }
            } catch (SQLException throwables) {
                departmentMap = new HashMap<>();
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new ERModelExceptions(ERModelExceptions.Status.READ_FAILURE,ERModelExceptions.Status.CONNECTION_CLOSING_FAILURE);
                }
                throw new ERModelExceptions(ERModelExceptions.Status.READ_FAILURE);
            }
            try(Statement statement = connection.createStatement()){
                String sql = "SELECT e.emp_id, e.name, p.basic_pay, e.start, c.department_id, c.department_name " +
                             "FROM employee e JOIN payroll p ON e.emp_id = p.emp_id " +
                             "JOIN (SELECT ed.employee_id, ed.department_id, d.department_name " +
                             "FROM employee_department_table ed JOIN department d ON ed.department_id = d.department_id " +
                             ") c ON e.emp_id = c.employee_id;";
                ResultSet resultSet = statement.executeQuery(sql);
                while(resultSet.next()){
                    int emp_id = resultSet.getInt("emp_id");
                    int department_id = resultSet.getInt("department_id");
                    if(employeePayrollDataList.size() == 0
                    || (employeePayrollDataList.get(employeePayrollDataList.size()-1).getId() != emp_id)){
                        String emp_name = resultSet.getString("name");
                        double salary = resultSet.getDouble("basic_pay");
                        LocalDate startDate = resultSet.getDate("start").toLocalDate();
                        EmployeePayrollData employeePayrollData =
                                new EmployeePayrollData(emp_id, emp_name, salary, startDate);
                        employeePayrollData.addDepartment(departmentMap.get(department_id));
                        employeePayrollDataList.add(employeePayrollData);
                        }
                    else
                        employeePayrollDataList.get(employeePayrollDataList.size()-1)
                                               .addDepartment(departmentMap.get(department_id));
                    }
                } catch (SQLException e) {
                departmentMap = new HashMap<>();
                try {
                    connection.close();
                } catch (SQLException throwables) {
                    throw new ERModelExceptions(ERModelExceptions.Status.READ_FAILURE, ERModelExceptions.Status.CONNECTION_CLOSING_FAILURE);
                }
                throw new ERModelExceptions(ERModelExceptions.Status.READ_FAILURE);
            }
            return employeePayrollDataList;
        }

    public int updateEmployeeData(String name, double salary) throws ERModelExceptions {
            Connection connection = null;
            try{
                connection = this.getConnection();
            } catch (SQLException e) {
                throw new ERModelExceptions(ERModelExceptions.Status.CONNECTION_FAILURE);
            }
            try(Statement statement = connection.createStatement()){
                double deductions = salary * 0.2;
                double tax = (salary - deductions) * 0.1;
                String sql = String.format("UPDATE payroll " +
                                          "INNER JOIN employee " +
                                          "ON payroll.emp_id = employee.emp_id " +
                                          "SET basic_pay = %s, deductions = %s, tax = %s " +
                                          "WHERE employee.name = '%s';",
                                           salary, deductions, tax, name);
                int result = statement.executeUpdate(sql);
                return result;
            } catch (SQLException e) {
                try {
                    connection.close();
                } catch (SQLException throwables) {
                    throw new ERModelExceptions(ERModelExceptions.Status.UPDATION_FAILURE,
                                                ERModelExceptions.Status.CONNECTION_CLOSING_FAILURE);
                }
                throw new ERModelExceptions(ERModelExceptions.Status.UPDATION_FAILURE);
            }
    }

    public List<EmployeePayrollData> getEmployeePayrollData(String name) throws ERModelExceptions {
            List<EmployeePayrollData> employeePayrollDataList = new ArrayList<>();
            if(preparedStatement == null)
                this.prepareStatement();
            try{
                preparedStatement.setString(1, name);
                ResultSet resultSet = preparedStatement.executeQuery();
                while(resultSet.next()){
                    int emp_id = resultSet.getInt("emp_id");
                    String emp_name = resultSet.getString("name");
                    LocalDate startDate = resultSet.getDate("start").toLocalDate();
                    double salary = resultSet.getDouble("basic_pay");
                    EmployeePayrollData empData = new EmployeePayrollData(emp_id, emp_name, salary, startDate);
                    employeePayrollDataList.add(empData);
                }
            } catch (SQLException e) {
                throw new ERModelExceptions(ERModelExceptions.Status.READ_FAILURE);
            }
            return employeePayrollDataList;
    }


}


