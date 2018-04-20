package azkaban.jobtype;

import azkaban.jobExecutor.Job;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;

import java.util.List;

import org.apache.log4j.Logger;

public class FlowBreakerCheckJob implements Job {
  
  public static final String JOB_NEXT_STEP = "job.next.step";
  protected volatile Props jobProps;
  protected final Logger logger;
  private final String jobId;

  public FlowBreakerCheckJob(final String jobid, final Props props, final Props jobProps, final Logger log) {
    this.jobId = jobid;
    this.jobProps = jobProps;
    this.logger = log;
  }

  @Override
  public void run() throws Exception {
    try {
      resolveProps();
    } catch (final Exception e) {
      handleError("Bad property definition! " + e.getMessage(), e);
    }

    final List<String> nextSteps = this.jobProps.getStringList(JOB_NEXT_STEP);

    if (nextSteps.contains(getId())) {
      this.logger.info("Continuing with this flow");
    } else {
      throw new Exception(String.format("Failing because there's no match between jobId=%s and property %s=%s", getId(), JOB_NEXT_STEP, nextSteps.toString()));
    }
  }

  @Override
  public String getId() {
    return this.jobId;
  }

  @Override
  public void cancel() throws Exception {
  }

  @Override
  public double getProgress() throws Exception {
    return 0;
  }

  @Override
  public Props getJobGeneratedProperties() {
    return new Props();
  }

  @Override
  public boolean isCanceled() {
    return false;
  }
  
  protected void resolveProps() {
    this.jobProps = PropsUtils.resolveProps(this.jobProps);
  }

  protected void handleError(final String errorMsg, final Exception e) throws Exception {
    this.logger.error(errorMsg);
    if (e != null) {
      throw new Exception(errorMsg, e);
    } else {
      throw new Exception(errorMsg);
    }
  }
}