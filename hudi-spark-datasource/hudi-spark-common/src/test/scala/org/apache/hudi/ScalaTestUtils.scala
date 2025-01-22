package org.apache.hudi

object ScalaTestUtils {
  
  /**
   * Utility method to run a test multiple times. For using the test call the method on the test block.
   * Example:
   * repeatTest(5) {
   *   ... // test code
   * }
   *
   * @param numRuns Number of test runs
   * @param test    Test to run
   */
  def repeatTest(numRuns: Int)(test: => Unit): Unit = {
    for (_ <- 1 to numRuns) {
      test
    }
  }
}
