package com.employeepayroll.ermodel;

import java.util.ArrayList;
import java.util.List;

public class ERModelExceptions extends Exception{
    enum Status{
        READ_FAILURE ("Unable to read data from data base"),
        CONNECTION_FAILURE ("Unable to establish connection"),
        CONNECTION_CLOSING_FAILURE ("Unable to close the connection"),
        UPDATION_FAILURE ("Unable to update"),
        TRANSACTION_FAILURE ("Unable to perform the transaction");

        private String message;

        Status(String message){
            this.message = message;
        }

        public String getMessage() { return message; }
    }
    List<Status> type;

    public ERModelExceptions(Status type){
        super(type.toString() + type.message);
        this.type = new ArrayList<>();
        this.type.add(type);
    }

    public ERModelExceptions(Status type1, Status type2){
        super(type1.toString() + ": " + type1.getMessage() + "\n"
        + type2.toString() + ": " + type2.getMessage());
        this.type = new ArrayList<>();
        this.type.add(type1);
        this.type.add(type2);
    }
}
