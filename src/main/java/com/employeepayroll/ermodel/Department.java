package com.employeepayroll.ermodel;

import java.util.Objects;

public class Department {
    int department_id;
    String department_name;

    public Department(int department_id, String department_name){
        this.department_id = department_id;
        this.department_name = department_name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Department that = (Department) o;
        return department_id == that.department_id;
    }

}
