package ru.ffyud.trials.csvdata;

import java.util.*;

/**
 * Created on 11/02/2018 by ssko.
 */
public final class ReportOne {
  private final Map<String, List<String>> data = new HashMap<>();

  void addUserAndForm(String userId, String formId) {
    if (data.containsKey(userId)) {
      data.get(userId).add(formId);
    } else {
      List<String> l = new ArrayList<>();
      l.add(formId);
      data.put(userId, l);
    }
  }

  public Set<String> getUsers() {
    return Collections.unmodifiableSet(data.keySet());
  }

  public List<String> usedForms(String userId) {
    return Collections.unmodifiableList(data.get(userId));
  }
}
