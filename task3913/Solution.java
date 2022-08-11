package com.javarush.task.task39.task3913;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class Solution {
    public static void main(String[] args) throws IOException {
        LogParser logParser = new LogParser(Paths.get("E:\\Projects\\Java\\JavaRushTasks\\4.JavaCollections\\src\\com\\javarush\\task\\task39\\task3913\\logs"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        System.out.println(logParser.execute("get date for event = \"SOLVE_TASK\" and date between \"19.03.2002 0:00:00\" and \"11.12.2017 23:59:59\""));

    }
}