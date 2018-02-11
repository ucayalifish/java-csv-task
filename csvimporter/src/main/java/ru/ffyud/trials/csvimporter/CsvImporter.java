package ru.ffyud.trials.csvimporter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.ffyud.trials.csvdata.DataService;
import ru.ffyud.trials.csvdata.Fields;

import java.io.FileReader;
import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@SpringBootApplication
public class CsvImporter implements ApplicationRunner {

  private static final Logger logger = LoggerFactory.getLogger(CsvImporter.class);

  @Bean
  DataService dataService() {
    return new DataService(jdbcTemplate);
  }

  @Autowired
  public CsvImporter(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  private final JdbcTemplate jdbcTemplate;

  // размер тестовых данных столь велик, что я решил накопить их в памяти.
  // Если их станет больше, то такой буфер так и так потребуется.
  private final List<CSVRecord> inMemBuffer = new ArrayList<>();

  private void processCSVRecord(final CSVRecord record) {
    inMemBuffer.add(record);
  }

  private final static String InsertSQL =
      "INSERT INTO raw_data "
      + "(ssoid, ts, grp, atype, asubtype, url, orgid, formid, code, ltpa, sudirresponse, ymdh) "
      + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

  private long batchInMemBuffer() {

    final int batchSize = 500;
    long total = 0;
    int start = 0;
    do {
      final List<CSVRecord> batch = inMemBuffer.stream().skip(start).limit(batchSize).collect(Collectors.toList());
      total += doBatch(batch, batch.size());
      start += batch.size();
    } while (total < inMemBuffer.size());
    return total;
  }

  private long doBatch(final List<CSVRecord> batch, final int batchSize) {
    int[] rr = jdbcTemplate.batchUpdate(InsertSQL, new BatchPreparedStatementSetter() {
      @Override
      public void setValues(PreparedStatement ps, int i) throws SQLException {
        final CSVRecord r = batch.get(i);
        ps.setString(1, r.get(Fields.ssoid));
        ps.setString(2, r.get(Fields.ts));
        ps.setString(3, r.get(Fields.grp));
        ps.setString(4, r.get(Fields.type));
        ps.setString(5, r.get(Fields.subtype));
        ps.setString(6, r.get(Fields.url));
        ps.setString(7, r.get(Fields.orgid));
        ps.setString(8, r.get(Fields.formid));
        ps.setString(9, r.get(Fields.code));
        ps.setString(10, r.get(Fields.ltpa));
        ps.setString(11, r.get(Fields.sudirresponse));
        ps.setString(12, r.get(Fields.ymdh));
      }

      @Override
      public int getBatchSize() {
        if (batchSize > batch.size()) {
          return batch.size();
        }
        return batchSize;
      }
    });

    final long ret = Arrays.stream(rr).sum();
    logger.info("{} items saved", ret);
    return ret;
  }

  private boolean recordIsUseless(CSVRecord r) {
    final String ssoid = r.get(Fields.ssoid);
    if (ssoid == null || ssoid.startsWith("Unauthorized") || ssoid.equals("")) {
      return true;
    }
    final String formid = r.get(Fields.formid);
    return formid == null || formid.equals("") || formid.startsWith("null");
  }

  @Override
  public void run(ApplicationArguments args) throws Exception {
    final String sourcePath = args.getOptionValues("data.csv").get(0);
    logger.info("Start processing '{}'", sourcePath);
    final DataService ds = dataService();
    ds.prepareTables();
    CSVParser parser = null;
    try {
      final Reader in = new FileReader(sourcePath);
      final CSVFormat format = CSVFormat.EXCEL.withHeader(Fields.class)
                                              .withDelimiter(';')
                                              .withSkipHeaderRecord(true);
      parser = new CSVParser(in, format);
      int total = 0;
      int rejected = 0;
      for (final CSVRecord record : parser) {
        total += 1;
        if (recordIsUseless(record)) {
          rejected += 1;
        } else {
          processCSVRecord(record);
        }
      }
      logger.info("{} records seen, {} records rejected", total, rejected);

      final long saved = batchInMemBuffer();

      logger.info("{} records imported", saved);
      ds.saveSummary(total, saved, rejected);
    } finally {
      if (parser != null) {
        parser.close();
      }
    }
  }

  public static void main(String[] args) {
    SpringApplication.run(CsvImporter.class, args);
  }
}
