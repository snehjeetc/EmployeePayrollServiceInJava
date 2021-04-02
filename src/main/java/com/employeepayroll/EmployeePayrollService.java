package com.employeepayroll;

import java.time.LocalDate;
import java.util.*;

public class EmployeePayrollService {

    enum IOService{ CONSOLE_IO, FILE_IO, DB_IO, REST_IO }
    private List<EmployeePayrollData> employeePayrollList;
    private final EmployeePayrollDBService employeePayrollDBService;

    public EmployeePayrollService() {
        employeePayrollDBService = EmployeePayrollDBService.getInstance();
    }

    public EmployeePayrollService(List<EmployeePayrollData> empList) {
        this();
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
        else
            System.out.println(employeePayrollList);
    }

    public long countEntries(IOService ioService) {
        if(ioService.equals(IOService.FILE_IO))
            return new EmployeePayrollFileIOService().countEntries();
        return employeePayrollList.size();
    }

    public ArrayList<EmployeePayrollData> readData(IOService ioService) {
        if(ioService.equals(IOService.FILE_IO)){
            employeePayrollList = new EmployeePayrollFileIOService().readData();
            return new ArrayList<>(employeePayrollList);
        }
        if(ioService.equals(IOService.DB_IO)){
            employeePayrollList = employeePayrollDBService.readData();
            return new ArrayList<>(employeePayrollList);
        }
        return null;
    }

    public void updateEmployeeSalary(String name, double salary, int choice){
        int result = (choice == 1) ? employeePayrollDBService.updateEmployeeData(name, salary)
                : employeePayrollDBService.updateEmployeeDataPreparedStatement(name, salary);
        if(result == 0) return;
        EmployeePayrollData employeePayrollData = this.getEmployeePayrollData(name);
        if(employeePayrollData != null) employeePayrollData.salary = salary;
    }

    public void addEmployeeToPayroll(String name, double salary, LocalDate startDate, String gender) {
        employeePayrollList.add(employeePayrollDBService.addEmployeeToPayroll(name, salary, startDate, gender));
    }


    public void addEmployeesToPayroll(List<EmployeePayrollData> employeePayrollDataList) {
        employeePayrollDataList.forEach(employeePayrollData -> {
            this.addEmployeeToPayroll(employeePayrollData.name, employeePayrollData.salary,
                    employeePayrollData.startDate, employeePayrollData.gender);
        });
    }

    public void addEmployeesToPayrollWithThreads(List<EmployeePayrollData> employeePayrollList){
        Map<Integer, Boolean> employeeAdditionStatus = new HashMap<>();
        employeePayrollList.forEach(employeePayrollData ->  {
            Runnable task = () -> {
                employeeAdditionStatus.put(employeePayrollData.hashCode(), false);
                System.out.println("Employee Being added: "+Thread.currentThread().getName());
                this.addEmployeeToPayroll(employeePayrollData.name, employeePayrollData.salary,
                        employeePayrollData.startDate, employeePayrollData.gender);
                employeeAdditionStatus.put(employeePayrollData.hashCode(), true);
                System.out.println("Employee added: "+ Thread.currentThread().getName());
            };
            Thread thread = new Thread(task, employeePayrollData.name);
            thread.start();
        });
        while(employeeAdditionStatus.containsValue(false)){
            try{Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private EmployeePayrollData getEmployeePayrollData(String name){
        return this.employeePayrollList.stream()
                                .filter(employeePayrollDataItem -> employeePayrollDataItem.name.equals(name))
                                .findFirst()
                                .orElse(null);
    }

    public List<EmployeePayrollData> getEmployeePayrollDataBetweenDates(IOService ioService, String from, String to){
        if(ioService.equals(IOService.DB_IO)){
            return employeePayrollDBService.getEmployeePayrollDataBetweenDates(from, to);
        }
        return null;
    }

    public boolean checkEmployeePayrollInSyncWithDB(String name) {
        List<EmployeePayrollData> employeePayrollDataList = employeePayrollDBService.getEmployeePayrollData(name);
        return employeePayrollDataList.get(0).equals(getEmployeePayrollData(name));
    }

    public List<String> calculateSumAverageMinMax(IOService ioService) {
        if(ioService.equals(IOService.DB_IO)){
            return employeePayrollDBService.calculateSumAverageMinMax();
        }
        return null;
    }

    public Map<String, List<Double>> calculateSumAverageMinMax_GroupByGender(IOService ioService) {
        if(ioService.equals(IOService.DB_IO)){
            return employeePayrollDBService.calculateSumAverageMinMax_GroupByGender();
        }
        return null;
    }
}
