package com.employeepayroll;

import java.time.LocalDate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EmployeePayrollData {
    int id;
    String name;
    double salary;
    LocalDate startDate;

    public EmployeePayrollData(int id, String name, double salary){
        this.id = id;
        this.name = name;
        this.salary = salary;
    }

    public EmployeePayrollData(int id, String name, double salary, LocalDate startDate){
        this(id, name, salary);
        this.startDate = startDate;
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

    @Override
    public String toString(){
        return "Emp id: " + this.id + ", Name: " + this.name + ", Salary: " + this.salary;
    }
}
