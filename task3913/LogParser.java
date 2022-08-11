package com.javarush.task.task39.task3913;

import com.javarush.task.task39.task3913.query.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class LogParser implements IPQuery,
        UserQuery,
        DateQuery,
        EventQuery,
        QLQuery {

    List<String> logStrings = new ArrayList<>();
    List<Log> logs = new ArrayList<>();
    SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");

    // When we create a new object of LogParser class we immediately scan the directory we got as parameter
    // and read all files that are in that directory to logStrings
    public LogParser(Path logDir) {
        try {
            Set<Path> set = Files.walk(Paths.get(String.valueOf(logDir)))
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".log"))
                    .collect(Collectors.toSet());
            for (Path path: set) {
                logStrings.addAll(Files.readAllLines(path));
            }
            parseLogs();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // We take file content we put in logStrings by lines, parse them to private class log
    // and write them to logs list
    private void parseLogs() {
        for (String log : logStrings) {
            String[] parts = log.split("\\t");
            String ip = parts[0];
            String username = parts[1];
            String date = parts[2];
            String event = parts[3];
            Status status = Status.valueOf(parts[4]);
            Log l;
            try {
                l = new Log(dateFormat.parse(date), ip, username, event, status);
            } catch (ParseException e) {
                throw new IllegalArgumentException();
            }
            logs.add(l);
        }
    }


    @Override
    public Set<Object> execute(String query) {
        Query parseQuery = new Query(query);
        switch (parseQuery.getField1()) {
            case ("ip") :
                return parseQuery.getLogsSelected().stream()
                        .map(log -> log.ip)
                        .collect(Collectors.toSet());
            case "user" :
                return parseQuery.getLogsSelected().stream()
                        .map(log -> log.username)
                        .collect(Collectors.toSet());
            case "date" :
                return parseQuery.getLogsSelected().stream()
                        .map(log -> log.date)
                        .collect(Collectors.toSet());
            case "event" :
                return parseQuery.getLogsSelected().stream()
                        .map(log -> log.event)
                        .collect(Collectors.toSet());
            case "status" :
                return parseQuery.getLogsSelected().stream()
                        .map(log -> log.status)
                        .collect(Collectors.toSet());
            default:
                return null;
        }

    }


    private static class Log {
        private final Date date;
        private final String ip;
        private final String username;
        private final Event event;
        private final Integer eventTaskNumber;
        private final Status status;

        public Log(Date date, String ip, String username, String event, Status status) {
            this.date = date;
            this.ip = ip;
            this.username = username;
            if(Event.valueOf(event.split(" ")[0]).equals(Event.SOLVE_TASK)
                    || Event.valueOf(event.split(" ")[0]).equals(Event.DONE_TASK)) {
                this.event = Event.valueOf(event.split(" ")[0]);
                this.eventTaskNumber = Integer.valueOf(event.split(" ")[1]);
            }  else {
                this.event = Event.valueOf(event);
                this.eventTaskNumber = null;
            }
            this.status = status;
        }

        public Integer getEventTaskNumber() {
            return eventTaskNumber;
        }
    }

    // a method to check if a date that is in log in the selected time frame
    private boolean isBetween(Date date, Date after, Date before) {
        return (after == null || date.after(after))
                &&
                (before == null || date.before(before) );
    }



    // This class is used to handle specific type of queries such as:
    //1) get ip for user = "Vasya"
    //2) get user for event = "DONE_TASK"
    //3) get event for date = "03.01.2014 03:45:23"
    //4) get ip for user = "Eduard Petrovich Morozko"
    //   and date between "11.12.2013 0:00:00" and "03.01.2014 23:59:59"
    private class Query {
        private String field1;
        private String field2;
        private String value1;
        private Date before = null;
        private Date after = null;
        private String regexLongWithDates = "get (ip|user|event|status|date) for (ip|user|event|status|date) = \"(.*?)\" and date between \"(.*?)\" and \"(.*?)\"";
        private String regexLong = "get (ip|user|event|status|date) for (ip|user|event|status|date) = \"(.*?)\"$";
        private String regexShort = "get (ip|user|event|status|date)$";
        private List<Log> logsSelected = logs;


        // shitcode I know but can't really think of smth else at this point(((((((
      public Query(String string) {
          if (string.matches(regexLongWithDates)){
              field1 = string.split(" ")[1];
              field2 = string.split(" ")[3];
              value1 = string.split("\"")[1];
              try {
                  after =  dateFormat.parse(string.split("\"")[3].replaceAll("\"", ""));
                  before = dateFormat.parse(string.split("\"")[5].replaceAll("\"", ""));
                  logsSelected = selector();
              } catch (ParseException e) {
                  e.printStackTrace();
              }
          } else if(string.matches(regexLong)) {
              String[] elements = string.split(" ");
              field1 = elements[1];
              field2 = elements[3];
              value1 = string.substring(string.indexOf("\"")).replaceAll("\"", "");
              try {
                  logsSelected = selector();
              } catch (ParseException e) {
                  e.printStackTrace();
              }
          } else if(string.matches(regexShort)){
              field1 = string.split(" ")[1];
          } else {
              System.out.println("Wrong syntax!");
          }
      }

        public List<Log> selector() throws ParseException {
            switch (field2) {
                case ("ip") :
                    return logs.stream()
                            .filter(log -> log.ip.equals(value1)
                            && isBetween(log.date, after, before))
                            .collect(Collectors.toList());
                case "user" :
                    return logs.stream()
                            .filter(log -> log.username.equals(value1)
                            && isBetween(log.date, after, before))
                            .collect(Collectors.toList());
                case "date" :
                    Date valueDate = dateFormat.parse(value1);
                        return logs.stream()
                                .filter(log -> log.date.equals(valueDate)
                                && isBetween(log.date, after, before))
                                .collect(Collectors.toList());
                case "event" :
                    return logs.stream()
                            .filter(log -> log.event.equals(Event.valueOf(value1))
                            && isBetween(log.date, after, before))
                            .collect(Collectors.toList());
                case "status" :
                    return logs.stream()
                            .filter(log -> log.status.equals(Status.valueOf(value1))
                            && isBetween(log.date, after, before))
                            .collect(Collectors.toList());
                default:
                    return null;
            }
        }

        public List<Log> getLogsSelected() {
            return logsSelected;
        }

        public String getField1() {
            return field1;
        }
    }














    /*
    IMPORTANT_IMPORTANT_IMPORTANT_IMPORTANT_IMPORTANT_IMPORTANT_IMPORTANT

    !!!!Depricated!!!! peace of code. The general idea of this task was to show just how many
    methods it would take to implement all possible requests, and then to
    create our own very simple query language to handle it


     */

    // We use getUniqueIPs method to get all unique ips for selected period of time and use method .size to find the amount
    @Override
    public int getNumberOfUniqueIPs(Date after, Date before) {
        return getUniqueIPs(after, before).size();
    }

    // We select number of unique ips for selected date; class set can only contain unique values
    @Override
    public Set<String> getUniqueIPs(Date after, Date before) {
        return logs.stream()
                .filter(log -> isBetween(log.date, after, before))
                .map(m -> m.ip)
                .collect(Collectors.toSet());
    }

    // We find unique ips for selected user in selected time period
    @Override
    public Set<String> getIPsForUser(String user, Date after, Date before) {
        return logs.stream()
                .filter(log -> log.username.equals(user)
                        && isBetween(log.date, after, before))
                .map(m -> m.ip)
                .collect(Collectors.toSet());
    }

    // We find unique ips for selected event in selected time period
    @Override
    public Set<String> getIPsForEvent(Event event, Date after, Date before) {
        return logs.stream()
                .filter(log -> log.event.equals(event)
                        && isBetween(log.date, after, before))
                .map(m -> m.ip)
                .collect(Collectors.toSet());
    }

    // We find unique ips for selected status in selected time period
    @Override
    public Set<String> getIPsForStatus(Status status, Date after, Date before) {
        return logs.stream()
                .filter(log -> log.status.equals(status)
                        && isBetween(log.date, after, before))
                .map(m -> m.ip)
                .collect(Collectors.toSet());
    }


    //DOWN BELOW ARE METHODS OF UserQuery interface!!!!



    // We get all usernames we have in files
    @Override
    public Set<String> getAllUsers() {

        return logs.stream()
                .map(m -> m.username)
                .collect(Collectors.toSet());
    }

    // Number of users that fit into time frame
    @Override
    public int getNumberOfUsers(Date after, Date before) {
        return logs.stream()
                .filter(log -> isBetween(log.date, after, before))
                .map(m -> m.username)
                .collect(Collectors.toSet()).size();
    }

    // number of unique events by user in time frame
    @Override
    public int getNumberOfUserEvents(String user, Date after, Date before) {
        return logs.stream()
                .filter(log -> log.username.equals(user)
                        && isBetween(log.date, after, before))
                .map(m -> m.event)
                .collect(Collectors.toSet()).size();
    }

    // get unique users by ip in time frame
    @Override
    public Set<String> getUsersForIP(String ip, Date after, Date before) {
        return logs.stream()
                .filter(log -> log.ip.equals(ip)
                        && isBetween(log.date, after, before))
                .map(m -> m.username)
                .collect(Collectors.toSet());
    }

    // users that had event LOGIN in time frame
    @Override
    public Set<String> getLoggedUsers(Date after, Date before) {
        return logs.stream()
                .filter(log -> log.event.equals(Event.LOGIN)
                        && isBetween(log.date,after, before))
                .map(m -> m.username)
                .collect(Collectors.toSet());
    }

    // users that had event DOWNLOAD_PLUGIN in time frame
    @Override
    public Set<String> getDownloadedPluginUsers(Date after, Date before) {
        return logs.stream()
                .filter(log -> log.event.equals(Event.DOWNLOAD_PLUGIN)
                        && isBetween(log.date,after, before))
                .map(m -> m.username)
                .collect(Collectors.toSet());
    }

    // users that had event WRITE_MESSAGE in time frame
    @Override
    public Set<String> getWroteMessageUsers(Date after, Date before) {
        return logs.stream()
                .filter(log -> log.event.equals(Event.WRITE_MESSAGE)
                        && isBetween(log.date,after, before))
                .map(m -> m.username)
                .collect(Collectors.toSet());
    }

    // users that had event SOLVE_TASK in time frame
    @Override
    public Set<String> getSolvedTaskUsers(Date after, Date before) {
        return logs.stream()
                .filter(log -> log.event.equals(Event.SOLVE_TASK)
                        && isBetween(log.date,after, before))
                .map(m -> m.username)
                .collect(Collectors.toSet());
    }

    // users that had event SOLVE_TASK and specific task number in time frame
    @Override
    public Set<String> getSolvedTaskUsers(Date after, Date before, int task) {
        return logs.stream()
                .filter(log -> log.event.equals(Event.SOLVE_TASK)
                        && isBetween(log.date,after, before)
                        && log.eventTaskNumber == task)
                .map(m -> m.username)
                .collect(Collectors.toSet());
    }

    // users that had event DONE_TASK in time frame
    @Override
    public Set<String> getDoneTaskUsers(Date after, Date before) {
        return logs.stream()
                .filter(log -> log.event.equals(Event.DONE_TASK)
                        && isBetween(log.date,after, before))
                .map(m -> m.username)
                .collect(Collectors.toSet());
    }

    // users that had event DONE_TASK and specific task number in time frame
    @Override
    public Set<String> getDoneTaskUsers(Date after, Date before, int task) {
        return logs.stream()
                .filter(log -> log.event.equals(Event.DONE_TASK)
                        && isBetween(log.date,after, before)
                        && log.eventTaskNumber == task)
                .map(m -> m.username)
                .collect(Collectors.toSet());
    }

    //UserQuery interface method implementation ends here!!!!


    // BELOW ARE THE METHODS OF DateQuery INTERFACE

    @Override
    public Set<Date> getDatesForUserAndEvent(String user, Event event, Date after, Date before) {
        return logs.stream()
                .filter(log -> log.event.equals(event)
                        && log.username.equals(user)
                        && isBetween(log.date,after, before))
                .map(m -> m.date)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Date> getDatesWhenSomethingFailed(Date after, Date before) {
        return logs.stream()
                .filter(log -> log.status.equals(Status.FAILED)
                        && isBetween(log.date,after, before))
                .map(m -> m.date)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Date> getDatesWhenErrorHappened(Date after, Date before) {
        return logs.stream()
                .filter(log -> log.status.equals(Status.ERROR)
                        && isBetween(log.date,after, before))
                .map(m -> m.date)
                .collect(Collectors.toSet());    }

    @Override
    public Date getDateWhenUserLoggedFirstTime(String user, Date after, Date before) {
        return logs.stream()
                .filter(log -> log.event.equals(Event.LOGIN)
                        && log.username.equals(user)
                        && isBetween(log.date,after, before))
                .map(m -> m.date)
                .collect(Collectors.toSet())
                .stream().sorted().min(Date::compareTo).orElse(null);
    }

    @Override
    public Date getDateWhenUserSolvedTask(String user, int task, Date after, Date before) {
        return logs.stream()
                .filter(log -> log.event.equals(Event.SOLVE_TASK)
                        && log.eventTaskNumber != null
                        && log.eventTaskNumber == task
                        && log.username.equals(user)
                        && isBetween(log.date,after, before))
                .map(m -> m.date)
                .collect(Collectors.toSet())
                .stream().sorted().min(Date::compareTo).orElse(null);    }

    @Override
    public Date getDateWhenUserDoneTask(String user, int task, Date after, Date before) {
        return logs.stream()
                .filter(log -> log.event.equals(Event.DONE_TASK)
                        && log.eventTaskNumber != null
                        && log.eventTaskNumber == task
                        && log.username.equals(user)
                        && isBetween(log.date,after, before))
                .map(m -> m.date)
                .collect(Collectors.toSet())
                .stream().sorted().min(Date::compareTo).orElse(null);
    }

    @Override
    public Set<Date> getDatesWhenUserWroteMessage(String user, Date after, Date before) {
        return logs.stream()
                .filter(log -> log.event.equals(Event.WRITE_MESSAGE)
                        && log.username.equals(user)
                        && isBetween(log.date,after, before))
                .map(m -> m.date)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Date> getDatesWhenUserDownloadedPlugin(String user, Date after, Date before) {
        return logs.stream()
                .filter(log -> log.event.equals(Event.DOWNLOAD_PLUGIN)
                        && log.username.equals(user)
                        && isBetween(log.date,after, before))
                .map(m -> m.date)
                .collect(Collectors.toSet());
    }

    // DateQuery interface implementation ends here!!!


    // IMPLEMENTATION of EventQuery interface

    @Override
    public int getNumberOfAllEvents(Date after, Date before) {
        return getAllEvents(after, before).size();
    }

    @Override
    public Set<Event> getAllEvents(Date after, Date before) {
        return logs.stream()
                .filter(log -> isBetween(log.date, after, before))
                .map(m -> m.event)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Event> getEventsForIP(String ip, Date after, Date before) {
        return logs.stream()
                .filter(log -> isBetween(log.date, after, before)
                        && log.ip.equals(ip))
                .map(m -> m.event)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Event> getEventsForUser(String user, Date after, Date before) {
        return logs.stream()
                .filter(log -> isBetween(log.date, after, before)
                        && log.username.equals(user))
                .map(m -> m.event)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Event> getFailedEvents(Date after, Date before) {
        return logs.stream()
                .filter(log -> isBetween(log.date, after, before)
                        && log.status.equals(Status.FAILED))
                .map(m -> m.event)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Event> getErrorEvents(Date after, Date before) {
        return logs.stream()
                .filter(log -> isBetween(log.date, after, before)
                        && log.status.equals(Status.ERROR))
                .map(m -> m.event)
                .collect(Collectors.toSet());
    }

    @Override
    public int getNumberOfAttemptToSolveTask(int task, Date after, Date before) {
        return logs.stream()
                .filter(log -> isBetween(log.date, after, before)
                        && log.event.equals(Event.SOLVE_TASK)
                        && log.eventTaskNumber == task)
                .map(m -> m.event)
                .collect(Collectors.toSet()).size();
    }

    @Override
    public int getNumberOfSuccessfulAttemptToSolveTask(int task, Date after, Date before) {
        return logs.stream()
                .filter(log -> isBetween(log.date, after, before)
                        && log.event.equals(Event.DONE_TASK)
                        && log.eventTaskNumber == task)
                .map(m -> m.event)
                .collect(Collectors.toSet()).size();
    }

    @Override
    public Map<Integer, Integer> getAllSolvedTasksAndTheirNumber(Date after, Date before) {
        Set<Integer> set = logs.stream()
                .filter(log -> isBetween(log.date, after, before)
                        && log.event.equals(Event.SOLVE_TASK))
                .map(m -> m.eventTaskNumber)
                .collect(Collectors.toSet());
        return set.stream().collect(Collectors.toMap(Integer::intValue, x -> set.size()));

    }

    @Override
    public Map<Integer, Integer> getAllDoneTasksAndTheirNumber(Date after, Date before) {
        return logs.stream()
                .filter(log -> isBetween(log.date, after, before)
                        && log.event.equals(Event.DONE_TASK))
                .map(m -> m.eventTaskNumber)
                .collect(Collectors.toSet())
                .stream().collect(Collectors.toMap(Integer::intValue,
                        x -> getNumberOfSuccessfulAttemptToSolveTask(x, after, before)));
    }


}