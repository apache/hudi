package org.apache.hudi.client;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;

@EnumDescription("Used to validate commits before writing.")
public enum PreCommitValidatorTypes {

  @EnumFieldDescription("Validator to run sql queries on new table state and expects a single result. "
      + "If the result does not match expected result, a validation error is thrown.")
  SQL_SINGLE_RESULT("org.apache.hudi.client.validator.SqlQuerySingleResultPreCommitValidator"),

  @EnumFieldDescription("Validator to run sql query and compare table state before new commit started and current inflight commit. "
      + "Expects both queries to return different results.")
  SQL_INEQUALITY("org.apache.hudi.client.validator.SqlQueryInequalityPreCommitValidator"),

  @EnumFieldDescription("Validator to run sql query and compare table state before new commit started and current inflight commit. "
    + "Expects both queries to return the same result.")
  SQL_EQUALITY("org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator"),

  @EnumFieldDescription("Use the validators set in `hoodie.precommit.validators`")
  CUSTOM("");

  public final String classPath;
  PreCommitValidatorTypes(String classPath) {
    this.classPath = classPath;
  }
}
