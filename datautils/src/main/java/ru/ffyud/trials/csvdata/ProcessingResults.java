package ru.ffyud.trials.csvdata;

public final class ProcessingResults {
  private final long total;
  private final long selected;
  private final long rejected;

  ProcessingResults(long total, long selected, long rejected) {
    this.total = total;
    this.selected = selected;
    this.rejected = rejected;
  }

  public long getTotal() {
    return total;
  }

  public long getSelected() {
    return selected;
  }

  public long getRejected() {
    return rejected;
  }
}
