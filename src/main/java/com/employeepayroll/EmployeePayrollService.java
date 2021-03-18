package com.employeepayroll;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class EmployeePayrollService {

    enum IOService{ CONSOLE_IO, FILE_IO, DB_IO, REST_IO; }
    private List<EmployeePayrollData> employeePayrollList;

    public EmployeePayrollService(List<EmployeePayrollData> empList) {
        this.employeePayrollList = empList;
    }

    public static void main(String[] args){
        ArrayList<EmployeePayrollData> employeePayrollDataList = new ArrayList<>();
        EmployeePayrollService employeePayrollService = new EmployeePayrollService(employeePayrollDataList);
        Scanner consoleInputReader = new Scanner(System.in);
        employeePayrollService.readEmployeePayrollData(consoleInputReader);
        employeePayrollService.writeEmployeePayrollData(IOService.CONSOLE_IO);
    }

    public void writeEmployeePayrollData(IOService ioService) {
        if(ioService.equals(IOService.CONSOLE_IO))
            System.out.println("Writing Employee payroll data to console:\n " + employeePayrollList);
        else if(ioService.equals(IOService.FILE_IO))
            new EmployeePayrollFileIOService().writeData(employeePayrollList);
    }

    public void readEmployeePayrollData(Scanner consoleInputReader) {
        System.out.println("Enter Employee Id: ");
        int id = consoleInputReader.nextInt();
        consoleInputReader.nextLine();
        System.out.println("Enter Employee name: ");
        String name = consoleInputReader.nextLine();
        System.out.println("Enter Employee Salary: ");
        double salary = consoleInputReader.nextDouble();
        consoleInputReader.nextLine();
        employeePayrollList.add(new EmployeePayrollData(id, name, salary));
    }

    public void printData(IOService ioService) {
        if(ioService.equals(IOService.FILE_IO))
            new EmployeePayrollFileIOService().printData();
    }

    public long countEntries(IOService ioService) {
        if(ioService.equals(IOService.FILE_IO))
            return new EmployeePayrollFileIOService().countEntries();
        return 0;
    }

    public ArrayList<EmployeePayrollData> readData(IOService ioService) {
        if(ioService.equals(IOService.FILE_IO)){
            employeePayrollList = new EmployeePayrollFileIOService().readData();
            return new ArrayList<>(employeePayrollList);
        }
        return null;
    }
}
