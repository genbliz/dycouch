import { FuseErrorUtilsService } from "./errors";

class QueryValidatorCheckBase {
  private queryErrorThrowChecks({ conditionValue, queryType }: { conditionValue: any; queryType: string }) {
    throw FuseErrorUtilsService.fuse_helper_createFriendlyError(
      `Value: ${JSON.stringify(conditionValue)}, is invalid for ${queryType} query`,
    );
  }

  beginWith(conditionValue: unknown) {
    if (!(conditionValue && typeof conditionValue === "string")) {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$beginsWith" });
    }
  }

  between(conditionValue: unknown) {
    if (!(conditionValue && Array.isArray(conditionValue) && conditionValue.length === 2)) {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$between" });
    }
  }

  contains(conditionValue: unknown) {
    if (!(conditionValue && typeof conditionValue === "string")) {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$contains" });
    }
  }

  notContains(conditionValue: unknown) {
    if (!(conditionValue && typeof conditionValue === "string")) {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$notContains" });
    }
  }

  in_query(conditionValue: unknown) {
    if (!(conditionValue && Array.isArray(conditionValue) && conditionValue.length)) {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$in" });
    }
  }

  notIn(conditionValue: unknown) {
    if (!(conditionValue && Array.isArray(conditionValue) && conditionValue.length)) {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$nin" });
    }
  }

  not_query(conditionValue: unknown) {
    if (!(conditionValue && typeof conditionValue === "object")) {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$not" });
    }
  }

  exists(conditionValue: unknown) {
    if (!(String(conditionValue) === "true" || String(conditionValue) === "false")) {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$exists" });
    }
  }

  or_query(conditionValue: unknown) {
    if (!(conditionValue && Array.isArray(conditionValue) && conditionValue.length)) {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$or" });
    }
  }

  and_query(conditionValue: unknown) {
    if (!(conditionValue && Array.isArray(conditionValue) && conditionValue.length)) {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$and" });
    }
  }

  throwQueryNotFound(queryType: any) {
    throw FuseErrorUtilsService.fuse_helper_createFriendlyError(
      `Query type: ${JSON.stringify(queryType)}, not supported`,
    );
  }
}

export const QueryValidatorCheck = new QueryValidatorCheckBase();
