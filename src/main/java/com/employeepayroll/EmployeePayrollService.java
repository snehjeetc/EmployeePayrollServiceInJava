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
        this.employeePayrollList = new ArrayList<>(empList);
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

    public void updateEmployees(List<EmployeePayrollData> employeePayrollDatas) {
        Map<Integer, Boolean> employeeUpdationStatus = new HashMap<>();
        employeePayrollDatas.forEach(employeePayrollData -> {
            Runnable task = () -> {
                employeeUpdationStatus.put(employeePayrollData.hashCode(), false);
                System.out.println("Employee being updated: " + Thread.currentThread().getName());
                int result = employeePayrollDBService.updateEmployeeDB(employeePayrollData.name, employeePayrollData.salary);
                if(result == 0) return;
                EmployeePayrollData payrollData = this.getEmployeePayrollData(employeePayrollData.name);
                if(payrollData != null) payrollData.salary = employeePayrollData.salary;
                employeeUpdationStatus.put(employeePayrollData.hashCode(), true);
                System.out.println("Employee updated : " + Thread.currentThread().getName());
            };
            Thread thread = new Thread(task, employeePayrollData.name);
            thread.start();
            while(employeeUpdationStatus.containsValue(false)){
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }


    public void addEmployeeToPayroll(String name, double salary, LocalDate startDate, String gender) {
        employeePayrollList.add(employeePayrollDBService.addEmployeeToPayroll(name, salary, startDate, gender));
    }

    public void addEmployeeToPayroll(EmployeePayrollData employeePayrollData, IOService ioService){
        if(ioService.equals(IOService.FILE_IO))
            this.addEmployeeToPayroll(employeePayrollData.name, employeePayrollData.salary,
                                     employeePayrollData.startDate, employeePayrollData.gender);
        else employeePayrollList.add(employeePayrollData);
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

    public void addEmployeeToPayroll_UsingDBThreads(List<EmployeePayrollData> asList) {
        Map<Integer, Boolean> employeeAdditionStatus = new HashMap<>();
        asList.forEach(employeePayrollData ->  {
            Runnable task = () -> {
                employeeAdditionStatus.put(employeePayrollData.hashCode(), false);
                System.out.println("Employee Being added: "+Thread.currentThread().getName());
                employeePayrollList.add(employeePayrollDBService.addEmployeeToPayroll_MulitThreadingConcept(employeePayrollData.name, employeePayrollData.salary,
                        employeePayrollData.startDate, employeePayrollData.gender));
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
        System.out.println("Employee Payroll list size: " + employeePayrollList.size());
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

    public boolean isSyncWithDB(List<EmployeePayrollData> employeePayrollDatas) {

        Map<Integer, Boolean> employeeCheckStatus = new HashMap<>();
        Map<String, Boolean> employeeCheckResult = new HashMap<>();
        employeePayrollDatas.forEach(employeePayrollData ->{
            Runnable task = () -> {
                employeeCheckStatus.put(employeePayrollData.hashCode(), false);
                employeeCheckResult.put(employeePayrollData.name,
                                        checkEmployeePayrollInSyncWithDB(employeePayrollData.name));
                employeeCheckStatus.put(employeePayrollData.hashCode(), true);
            };
            Thread thread = new Thread(task);
            thread.start();
        });
        while(employeeCheckStatus.containsValue(false)){
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if(employeeCheckResult.containsValue(false))
            return false;
        return true;
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
