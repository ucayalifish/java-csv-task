package ru.ffyud.trials.csvdata;

/**
 * Created on 11/02/2018 by ssko.
 */
public final class ReportTwoItem {
  private final String userId;
  private final String formId;
  private final String finalStep;

  ReportTwoItem(String userId, String formId, String finalStep) {
    this.userId = userId;
    this.formId = formId;
    this.finalStep = finalStep;
  }

  public String getUserId() {
    return userId;
  }

  public String getFormId() {
    return formId;
  }

  public String getFinalStep() {
    return finalStep;
  }
}
