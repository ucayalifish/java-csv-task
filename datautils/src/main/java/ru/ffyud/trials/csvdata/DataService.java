package ru.ffyud.trials.csvdata;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.Tuple4;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class DataService {
  private final JdbcTemplate jdbc;

  @Autowired
  public DataService(JdbcTemplate jdbcTemplate) {
    jdbc = jdbcTemplate;
  }

  public void prepareTables() {
    jdbc.execute("DROP TABLE raw_data;");
    jdbc.execute("DROP TABLE summary");
    jdbc.execute("CREATE TABLE raw_data\n"
                 + "(\n"
                 + "  ssoid         TEXT,\n"
                 + "  ts            TEXT,\n"
                 + "  grp           TEXT,\n"
                 + "  atype         TEXT,\n"
                 + "  asubtype      TEXT,\n"
                 + "  url           TEXT,\n"
                 + "  orgid         TEXT,\n"
                 + "  formid        TEXT,\n"
                 + "  code          TEXT,\n"
                 + "  ltpa          TEXT,\n"
                 + "  sudirresponse TEXT,\n"
                 + "  ymdh          TEXT\n"
                 + ");");
    jdbc.execute("CREATE TABLE summary\n"
                 + "(\n"
                 + "  total    BIGINT,\n"
                 + "  selected BIGINT,\n"
                 + "  rejected BIGINT\n"
                 + ");\n");
  }

  public void saveSummary(long total, long selected, long rejected) {
    jdbc.execute("DELETE FROM summary;");
    jdbc.update("INSERT INTO summary (total, selected, rejected) VALUES (?, ?, ?);", total, selected, rejected);
  }

  public ProcessingResults loadProcessingResults() {
    return jdbc.queryForObject("SELECT total, selected, rejected FROM summary LIMIT 1;", (row, i) -> {
      final long total = row.getLong("total");
      final long selected = row.getLong("selected");
      final long rejected = row.getLong("rejected");
      return new ProcessingResults(total, selected, rejected);
    });
  }

  public ReportOne reportOneData() {
    ReportOne ret = new ReportOne();

    List<Tuple2<String, String>> data = jdbc.query("SELECT ssoid, formid FROM raw_data"
                                                   + " GROUP BY ssoid, formid ORDER BY ssoid;",
                                                   (row, i) -> {
                                                     final String uid = row.getString("ssoid");
                                                     final String fid = row.getString("formid");
                                                     return Tuple.of(uid, fid);
                                                   });
    data.forEach(t -> ret.addUserAndForm(t._1, t._2));
    return ret;
  }

  public List<ReportTwoItem> reportTwo() {
    final List<ReportTwoItem> ret = new ArrayList<>();

    // получаем список завершивших успешно
    final List<String> succeeded = jdbc.query("SELECT ssoid FROM raw_data"
                                              + " WHERE grp LIKE 'dszn_%' AND asubtype = 'send'"
                                              + " ORDER BY ssoid;",
                                              (resultSet, i) -> resultSet.getString(1));

    // получаем максимальные таймстампы для каждого юзера, услуги, и формы, кроме тех, что успешно завершили работу
    final List<Tuple4> failures = jdbc.query("SELECT ssoid, grp, formid, max(ts::BIGINT) AS completed\n"
                                             + "FROM raw_data\n"
                                             + "WHERE grp LIKE 'dszn_%'\n"
                                             + "GROUP BY ssoid, grp, formid\n"
                                             + "ORDER BY ssoid;",
                                             (row, i) -> {
                                               final String ssoid = row.getString(1);
                                               final String grp = row.getString(2);
                                               final String formid = row.getString(3);
                                               final long ts = row.getLong(4);
                                               return Tuple.of(ssoid, grp, formid, ts);
                                             });

    //noinspection SuspiciousMethodCalls
    failures.stream().filter(t -> !succeeded.contains(t._1)).forEach(t -> {
      List<ReportTwoItem> l = jdbc.query("SELECT ssoid, grp, formid, atype, asubtype\n"
                                         + "FROM raw_data\n"
                                         + "WHERE ssoid=? AND grp = ? AND formid= ? AND ts=?;",
                                         (rs, i) -> {
                                           try {
                                             final String ssoid = rs.getString("ssoid");
                                             final String grp = rs.getString("grp");
                                             final String formid = rs.getString("formid");
                                             final String atype = rs.getString("atype");
                                             final String asubtype = rs.getString("asubtype");
                                             return new ReportTwoItem(ssoid,
                                                                      grp + ":" + formid,
                                                                      atype + ":" + asubtype);
                                           } catch (Exception e) {
                                             e.printStackTrace();
                                           }
                                           return null;
                                         },
                                         t._1, t._2, t._3, String.valueOf(t._4));
      ret.add(l.get(0)); // в данных реально есть дубликаты!
    });
    return ret;
  }

  public List<Map.Entry<String, Integer>> reportThree() {
    return jdbc.query("WITH form_counter AS (\n"
                      + "    SELECT\n"
                      + "      ssoid,\n"
                      + "      grp,\n"
                      + "      formid\n"
                      + "    FROM raw_data\n"
                      + "    WHERE ssoid <> 'Unauthorized'\n"
                      + "          AND ssoid IS NOT NULL\n"
                      + "          AND ssoid <> ''\n"
                      + "          AND formid IS NOT NULL\n"
                      + "          AND formid <> 'null'\n"
                      + "          AND formid <> ''\n"
                      + "    GROUP BY ssoid, grp, formid\n"
                      + "    ORDER BY grp, formid\n"
                      + ")\n"
                      + "SELECT\n"
                      + "  grp,\n"
                      + "  formid,\n"
                      + "  count(formid) AS fc\n"
                      + "FROM form_counter\n"
                      + "GROUP BY grp, formid\n"
                      + "ORDER BY fc DESC\n"
                      + "LIMIT 5;",
                      (rs, i) -> {
                        final String grp = rs.getString(1);
                        final String formid = rs.getString(2);
                        final int count = rs.getInt(3);
                        return new AbstractMap.SimpleEntry<>(grp + ":" + formid, count);
                      });
  }

}
