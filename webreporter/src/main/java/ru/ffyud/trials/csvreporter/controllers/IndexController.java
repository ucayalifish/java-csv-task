package ru.ffyud.trials.csvreporter.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import ru.ffyud.trials.csvdata.DataService;
import ru.ffyud.trials.csvdata.ProcessingResults;
import ru.ffyud.trials.csvdata.ReportTwoItem;

import java.util.List;
import java.util.Map;

@Controller
public class IndexController {

  private final DataService ds;

  @Autowired
  public IndexController(DataService dataService) {
    ds = dataService;
  }

  @RequestMapping("/")
  String index(Map<String, Object> model) {
    final ProcessingResults overall = ds.loadProcessingResults();
    model.put("total", overall.getTotal());
    model.put("selected", overall.getSelected());
    model.put("rejected", overall.getRejected());
    return "index";
  }

  @RequestMapping("/reportOne")
  String reportOne(Map<String, Object> model) {
    model.put("report", ds.reportOneData());
    return "report_one";
  }

  @RequestMapping("/reportTwo")
  String reportTwo(Map<String, Object> model) {
    List<ReportTwoItem> loosers = ds.reportTwo();
    model.put("loosers", loosers);
    return "report_two";
  }

  @RequestMapping("/reportThree")
  String reportThree(Map<String, Object> model) {
    List<Map.Entry<String, Integer>> topFive = ds.reportThree();
    model.put("topFive", topFive);
    return "report_three";
  }
}
