package com.employeepayroll.ermodel;

import com.employeepayroll.EmployeePayrollData;

import java.sql.*;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

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

        private Connection getConnection() throws ERModelExceptions {
            Connection connection = null;
            try {
                System.out.println("Connecting to database: " + jdbcUrl);
                connection = DriverManager.getConnection(jdbcUrl, userName, passWord);
                System.out.println("connection is successful!!!!" + connection);
                return connection;
            } catch (SQLException e) {
                throw new ERModelExceptions(ERModelExceptions.Status.CONNECTION_FAILURE);
            }
        }

        private void prepareStatement() throws ERModelExceptions {
                Connection connection = this.getConnection();
                String sql = "SELECT e.emp_id, e.name, e.start, p.basic_pay " +
                             "FROM employee e " +
                             "JOIN payroll p ON e.emp_id = p.emp_id " +
                             "WHERE e.name = ? AND e.isActive = true;";
            try {
                preparedStatement = connection.prepareStatement(sql);
            } catch (SQLException e) {
               throw new ERModelExceptions(ERModelExceptions.Status.READ_FAILURE);
            }
        }

        public List<EmployeePayrollData> readData(Map<Integer, Department> departmentMap) throws ERModelExceptions {
            List<EmployeePayrollData> employeePayrollDataList = new ArrayList<>();
            Connection connection = null;
            connection = getConnection();

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
                    throw new ERModelExceptions(ERModelExceptions.Status.READ_FAILURE,
                            ERModelExceptions.Status.CONNECTION_CLOSING_FAILURE);
                }
                throw new ERModelExceptions(ERModelExceptions.Status.READ_FAILURE);
            }
            try(Statement statement = connection.createStatement()){
                String sql = "SELECT e.emp_id, e.name, p.basic_pay, e.start, c.department_id, c.department_name " +
                             "FROM employee e JOIN payroll p ON e.emp_id = p.emp_id " +
                             "JOIN (SELECT ed.employee_id, ed.department_id, d.department_name " +
                             "FROM employee_department_table ed JOIN department d ON ed.department_id = d.department_id " +
                             ") c ON e.emp_id = c.employee_id " +
                             "WHERE e.isActive = true;";
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
                    throw new ERModelExceptions(ERModelExceptions.Status.READ_FAILURE,
                            ERModelExceptions.Status.CONNECTION_CLOSING_FAILURE);
                }
                throw new ERModelExceptions(ERModelExceptions.Status.READ_FAILURE);
            }
            return employeePayrollDataList;
        }

    public int updateEmployeeData(String name, double salary) throws ERModelExceptions {
            Connection connection = null;
            connection = this.getConnection();
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
            List<EmployeePayrollData> employeePayrollDataList = null;
            if(preparedStatement == null)
                this.prepareStatement();
            try{
                preparedStatement.setString(1, name);
                ResultSet resultSet = preparedStatement.executeQuery();
                employeePayrollDataList = this.executeSelectQuery(resultSet);
            } catch (SQLException e) {
                throw new ERModelExceptions(ERModelExceptions.Status.READ_FAILURE);
            }
            return employeePayrollDataList;
    }

    private List<EmployeePayrollData> executeSelectQuery(ResultSet resultSet) throws SQLException {
        List<EmployeePayrollData> employeePayrollDataList = new ArrayList<>();
        while(resultSet.next()){
            int emp_id = resultSet.getInt("emp_id");
            String emp_name = resultSet.getString("name");
            LocalDate startDate = resultSet.getDate("start").toLocalDate();
            double salary = resultSet.getDouble("basic_pay");
            EmployeePayrollData empData = new EmployeePayrollData(emp_id, emp_name, salary, startDate);
            employeePayrollDataList.add(empData);
        }
        return employeePayrollDataList;
    }

    public EmployeePayrollData addEmployeeToPayroll(String name, String gender, double salary, LocalDate startDate,
                                    Map<Integer, Department> departmentMap, String[] departmentNames) throws ERModelExceptions {
        int emp_id = -1;
        Connection connection = this.getConnection();
        try{
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new ERModelExceptions(ERModelExceptions.Status.TRANSACTION_FAILURE);
        }
        try(Statement statement = connection.createStatement()){
            String sql = String.format("INSERT INTO employee " +
                         "(name, gender, start) VALUES " +
                         "('%s', '%s', '%s');", name, gender, startDate);
            int rowAffected = statement.executeUpdate(sql, statement.RETURN_GENERATED_KEYS);
            if(rowAffected == 1){
                ResultSet resultSet = statement.getGeneratedKeys();
                if(resultSet.next()) emp_id = resultSet.getInt(1);
            }
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException throwables) {
                throw new ERModelExceptions(ERModelExceptions.Status.UPDATION_FAILURE,
                                            ERModelExceptions.Status.TRANSACTION_FAILURE);
            }
            try {
                connection.close();
            } catch (SQLException throwables) {
                throw new ERModelExceptions(ERModelExceptions.Status.UPDATION_FAILURE,
                                            ERModelExceptions.Status.CONNECTION_CLOSING_FAILURE);
            }
            throw new ERModelExceptions(ERModelExceptions.Status.UPDATION_FAILURE);
        }
        try(Statement statement = connection.createStatement()){
            double deductions = salary * 0.2;
            double tax = (salary - deductions) * 0.1;
            String sql = String.format("INSERT INTO payroll " +
                         "(emp_id, basic_pay, deductions, tax) VALUES " +
                         "(%s, %s, %s, %s);", emp_id, salary, deductions, tax);
            int rowAffected = statement.executeUpdate(sql);
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException throwables) {
                throw new ERModelExceptions(ERModelExceptions.Status.UPDATION_FAILURE,
                        ERModelExceptions.Status.TRANSACTION_FAILURE);
            }
            try {
                connection.close();
            } catch (SQLException throwables) {
                throw new ERModelExceptions(ERModelExceptions.Status.UPDATION_FAILURE,
                        ERModelExceptions.Status.CONNECTION_CLOSING_FAILURE);
            }
            throw new ERModelExceptions(ERModelExceptions.Status.UPDATION_FAILURE);
        }
        EmployeePayrollData employeePayrollData =
                new EmployeePayrollData(emp_id, name, salary, startDate);
        try {
            boolean isSuccess = this.updateDepartments(connection, employeePayrollData, departmentMap, departmentNames);
            if(!isSuccess)
                throw new SQLException();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException throwables) {
                throw new ERModelExceptions(ERModelExceptions.Status.UPDATION_FAILURE,
                                            ERModelExceptions.Status.TRANSACTION_FAILURE);
            }
            try {
                connection.close();
            } catch (SQLException throwables) {
                throw new ERModelExceptions(ERModelExceptions.Status.UPDATION_FAILURE,
                                            ERModelExceptions.Status.CONNECTION_CLOSING_FAILURE);
            }
            throw new ERModelExceptions(ERModelExceptions.Status.UPDATION_FAILURE);
        }
        try {
            connection.commit();
            return employeePayrollData;
        } catch (SQLException e) {
            throw new ERModelExceptions(ERModelExceptions.Status.TRANSACTION_FAILURE);
        }finally{
            try {
                connection.close();
            } catch (SQLException e) {
                throw new ERModelExceptions(ERModelExceptions.Status.CONNECTION_CLOSING_FAILURE);
            }
        }
    }

    private boolean updateDepartments(Connection connection, EmployeePayrollData employeePayrollData,
                                      Map<Integer, Department> departmentMap, String[] departmentNames) throws SQLException {
            if(departmentNames.length == 0)
                return true;
            List<Department> departmentList = new ArrayList<>();
            String select_query = "SELECT * FROM department WHERE department_name = ?";
            String update_query = "INSERT INTO department (department_name) VALUES (?)";
            PreparedStatement searchStatement = connection.prepareStatement(select_query);
            PreparedStatement updateStatement = connection.prepareStatement(update_query, Statement.RETURN_GENERATED_KEYS);
            int index = 0;
            while(index < departmentNames.length){
                searchStatement.setString(1, departmentNames[index]);
                ResultSet searchResultSet = searchStatement.executeQuery();
                int department_id = -1;
                if(searchResultSet.next()) department_id = searchResultSet.getInt("department_id");
                if(department_id == -1){
                    updateStatement.setString(1, departmentNames[index]);
                    int rowAffected = updateStatement.executeUpdate();
                    ResultSet updateResult = updateStatement.getGeneratedKeys();
                    if(updateResult.next()) department_id = updateResult.getInt(1);
                }
                Department department = new Department(department_id, departmentNames[index]);
                if(!departmentMap.containsKey(department_id))
                    departmentMap.put(department_id, department);
                departmentList.add(department);
                index++;
            }
            try(Statement statement = connection.createStatement()) {
                for (Department department : departmentList) {
                    String sql = String.format("INSERT INTO employee_department_table " +
                            "(employee_id, department_id) VALUES " +
                            "(%s, %s);", employeePayrollData.getId(), department.department_id);
                    statement.executeUpdate(sql);
                    employeePayrollData.addDepartment(department);
                }
            }
            return true;
    }

    public List<EmployeePayrollData> getEmployeePayrollDataBetweenDates(String from, String to) throws ERModelExceptions {
            LocalDate fromDate = LocalDate.parse(from);
            LocalDate toDate = (to == null) ? LocalDate.now() : LocalDate.parse(to);
            Connection connection = this.getConnection();
            try( Statement statement = connection.createStatement()){
                String sql = String.format("SELECT e.emp_id, e.name, e.start, p.basic_pay " +
                        "FROM employee e " +
                        "JOIN payroll p ON e.emp_id = p.emp_id " +
                        "WHERE e.start BETWEEN '%s' AND '%s';", fromDate, toDate);
                ResultSet resultSet = statement.executeQuery(sql);
                return this.executeSelectQuery(resultSet);
            } catch (SQLException e) {
                throw new ERModelExceptions(ERModelExceptions.Status.READ_FAILURE);
            }finally {
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new ERModelExceptions(ERModelExceptions.Status.CONNECTION_CLOSING_FAILURE);
                }
            }
    }

    public List<Double> calculateSumAverageMinMax() throws ERModelExceptions {
            String sql = "SELECT SUM(basic_pay), AVG(basic_pay), MIN(basic_pay), MAX(basic_pay) " +
                         "FROM payroll;";
           Connection connection = this.getConnection();
           List<Double> output = new ArrayList<>();
           try(Statement statement = connection.createStatement()){
               ResultSet resultSet = statement.executeQuery(sql);
               while(resultSet.next()){
                   output.add(resultSet.getDouble("SUM(basic_pay)"));
                   output.add(resultSet.getDouble("AVG(basic_pay)"));
                   output.add(resultSet.getDouble("MIN(basic_pay)"));
                   output.add(resultSet.getDouble("MAX(basic_pay)"));
               }
               return output;
           } catch (SQLException e) {
               throw new ERModelExceptions(ERModelExceptions.Status.READ_FAILURE);
           }finally {
               try {
                   connection.close();
               } catch (SQLException e) {
                   throw new ERModelExceptions(ERModelExceptions.Status.CONNECTION_CLOSING_FAILURE);
               }
           }
    }

    public Map<String, List<Double>> calculateSumAverageMinMax_GroupByGender() throws ERModelExceptions {
        Map<String, List<Double>> outputMap = new HashMap<>();
        Connection connection = this.getConnection();
        try(Statement statement = connection.createStatement()){
            String sql = "SELECT e.gender, SUM(p.basic_pay), AVG(p.basic_pay), MIN(p.basic_pay), MAX(p.basic_pay) " +
                         "FROM employee e JOIN payroll p USING (emp_id) " +
                         "GROUP BY e.gender;";
            ResultSet resultSet = statement.executeQuery(sql);
            while(resultSet.next()){
                String gender = resultSet.getString("gender");
                List<Double> output = new ArrayList<>();
                output.add(resultSet.getDouble("SUM(p.basic_pay)"));
                output.add(resultSet.getDouble("AVG(p.basic_pay)"));
                output.add(resultSet.getDouble("MIN(p.basic_pay)"));
                output.add(resultSet.getDouble("MAX(p.basic_pay)"));
                outputMap.put(gender, output);
            }
            return outputMap;
        } catch (SQLException e) {
            throw new ERModelExceptions(ERModelExceptions.Status.READ_FAILURE);
        }finally {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new ERModelExceptions(ERModelExceptions.Status.CONNECTION_CLOSING_FAILURE);
            }
        }
    }

    public int removeEmployee(String name) throws ERModelExceptions { 
            int rowAffected = 0;
            int emp_id = -1;
            Connection connection = this.getConnection();
            try{
                connection.setAutoCommit(false);
            } catch (SQLException e) {
                throw new ERModelExceptions(ERModelExceptions.Status.TRANSACTION_FAILURE);
            }
            try(Statement statement = connection.createStatement()){
                String sql = String.format("UPDATE employee SET isActive = false " +
                                           "WHERE name = '%s';", name);
                String sql_Select = String.format("SELECT emp_id FROM employee WHERE name = '%s'", name);
                ResultSet resultSet = statement.executeQuery(sql_Select);
                if(resultSet.next()) emp_id = resultSet.getInt("emp_id");
                rowAffected = statement.executeUpdate(sql);
            } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException throwables) {
                throw new ERModelExceptions(ERModelExceptions.Status.REMOVAL_FAILURE,
                                            ERModelExceptions.Status.TRANSACTION_FAILURE);
            }
            try {
                connection.close();
            } catch (SQLException throwables) {
                throw new ERModelExceptions(ERModelExceptions.Status.REMOVAL_FAILURE,
                                            ERModelExceptions.Status.CONNECTION_CLOSING_FAILURE);
            }
            throw new ERModelExceptions(ERModelExceptions.Status.REMOVAL_FAILURE);
        }
        try {
           this.removeFromDepartments(connection, emp_id);
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException throwables) {
                throw new ERModelExceptions(ERModelExceptions.Status.REMOVAL_FAILURE,
                        ERModelExceptions.Status.TRANSACTION_FAILURE);
            }
            try {
                connection.close();
            } catch (SQLException throwables) {
                throw new ERModelExceptions(ERModelExceptions.Status.REMOVAL_FAILURE,
                        ERModelExceptions.Status.CONNECTION_CLOSING_FAILURE);
            }
            throw new ERModelExceptions(ERModelExceptions.Status.REMOVAL_FAILURE);
        }
        try {
            connection.commit();
            return rowAffected;
        } catch (SQLException e) {
           throw new ERModelExceptions(ERModelExceptions.Status.TRANSACTION_FAILURE);
        }
    }

    private int removeFromDepartments(Connection connection, int employee_id) throws SQLException {
            if(employee_id == -1)
                return 0;
            int rowAffected = 0;
            try(Statement statement = connection.createStatement()){
                String sql = String.format("DELETE FROM employee_department_table " +
                        "WHERE employee_id = %s", employee_id);
                rowAffected = statement.executeUpdate(sql);
            }
            return rowAffected;
    }
}


