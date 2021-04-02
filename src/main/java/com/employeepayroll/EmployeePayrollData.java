package com.employeepayroll;

import com.employeepayroll.ermodel.Department;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EmployeePayrollData {
    int id;
    String name;
    double salary;
    String gender;
    LocalDate startDate;

    List<Department> departments;

    public EmployeePayrollData(int id, String name, double salary){
        this.id = id;
        this.name = name;
        this.salary = salary;
    }

    public EmployeePayrollData(int id, String name, double salary, LocalDate startDate){
        this(id, name, salary);
        this.startDate = startDate;
        departments = new ArrayList<>();
    }

    public EmployeePayrollData(int id, String name, String gender, double salary, LocalDate startDate){
        this(id, name, salary, startDate);
        this.gender = gender;
    }

    public static EmployeePayrollData extractEmployeePayrollObject(String line) {
        String[] fields = line.split(",");
        String[] storageFields = new String[fields.length];
        Pattern pattern = Pattern.compile("(?<=([:][\\s]))[0-9a-zA-Z.\\s]+");
        int index = 0;
        for(String field : fields){
            Matcher matcher = pattern.matcher(field);
            if(matcher.find())
                storageFields[index++] = matcher.group();
        }
        int id = Integer.parseInt(storageFields[0]);
        String name = storageFields[1];
        double salary = Double.parseDouble(storageFields[2]);
        return new EmployeePayrollData(id, name, salary);
    }

    public void addDepartment(Department department){
        departments.add(department);
    }

    public int getId() { return this.id; }
    public String getName() { return this.name; }
    public void setSalary(double salary) { this.salary = salary; }

    @Override
    public String toString(){
        return "Emp id: " + this.id + ", Name: " + this.name + ", Salary: " + this.salary;
    }

    @Override
    public boolean equals(Object o){
        if(this == o) return true;
        if(o == null || getClass() != o.getClass()) return false;
        EmployeePayrollData that = (EmployeePayrollData) o;
        return this.id == that.id &&
                Double.compare(this.salary, that.salary) == 0 &&
                this.name.equals(that.name);
    }

    @Override
    public int hashCode(){
        return Objects.hash(name, gender, salary, startDate);
    }
}
