package com.kailash.java.programming;

import com.opencsv.CSVReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

public class App {

    public static void main(String[] args) {
        if (args[0] != null) {
            try (CSVReader csvReader = new CSVReader(new FileReader(args[0]))) {
                String[] values;
                values = csvReader.readNext();
                DatabaseUtils.createTable(Arrays.asList(values));
                DatabaseUtils.createPreparedStatement(Arrays.asList(values));
                while ((values = csvReader.readNext()) != null) {
                    DatabaseUtils.processRecords(Arrays.asList(values));
                }
                System.out.println("ALl the records are inserted");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Please provide the argument while running jar (csv filename path)");
        }
    }


}
