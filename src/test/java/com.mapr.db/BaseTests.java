package com.mapr.db;

import com.mapr.db.policy.RetryPolicy;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;

@Slf4j
public class BaseTests {

  @Test
  public void testRetryPolicyBuilder() throws IOException {
    RetryPolicy policy = new RetryPolicy.Builder()
        .setNumOfReties(50)
        .setTimeout(100)
        .setAlternateSuffix(".foo")
        .setAlternateTable("/some/different/table")
        .build();
    log.info(policy.toString());
  }
}
