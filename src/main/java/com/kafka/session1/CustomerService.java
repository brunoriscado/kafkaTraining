package com.kafka.session1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bruno on 30/03/17.
 */
public class CustomerService {
    private Map<String, Integer> usersMap;

    public CustomerService() {
        usersMap = new HashMap<String, Integer>();
        usersMap.put("Tom", 0);
        usersMap.put("Mary", 1);
        usersMap.put("Alice", 2);
    }

    public Integer findUserById(String username) {
        return usersMap.get(username);
    }

    public List<String> findAllUsers() {
        return new ArrayList<String>(usersMap.keySet());
    }
}
