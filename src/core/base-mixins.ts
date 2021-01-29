import { GenericFriendlyError } from "../helpers/errors";
import { DynamoFilterQueryOperation } from "./dynamo-filter-query-operation";
import { DynamoQueryScanProcessor } from "./dynamo-query-scan-processor";

export interface DynamoFilterQueryMixin
  extends DynamoFilterQueryOperation,
    DynamoQueryScanProcessor {}

function applyMixins(derivedCtor: any, baseCtors: any[]) {
  baseCtors.forEach((baseCtor) => {
    Object.getOwnPropertyNames(baseCtor.prototype).forEach((name) => {
      Object.defineProperty(
        derivedCtor.prototype,
        name,
        Object.getOwnPropertyDescriptor(
          baseCtor.prototype,
          name
        ) as PropertyDescriptor
      );
    });
  });
}

export abstract class DynamoFilterQueryMixin {
  protected allHelpValidateRequiredNumber(keyValueValidates: {
    [key: string]: number;
  }) {
    const errors: string[] = [];
    Object.entries(keyValueValidates).forEach(([key, value]) => {
      if (!(!isNaN(Number(value)) && typeof value === "number")) {
        errors.push(`${key} is required`);
      }
    });
    if (errors.length) {
      throw new GenericFriendlyError(`${errors.join("; ")}.`);
    }
  }

  protected allHelpCreateFriendlyError(message: string, statusCode?: number) {
    return new GenericFriendlyError(message);
  }

  protected allHelpValidateRequiredString(keyValueValidates: {
    [key: string]: string;
  }) {
    const errors: string[] = [];
    Object.entries(keyValueValidates).forEach(([key, value]) => {
      if (!(value && typeof value === "string")) {
        errors.push(`${key} is required`);
      }
    });
    if (errors.length) {
      throw new GenericFriendlyError(`${errors.join("; ")}.`);
    }
  }
}

applyMixins(DynamoFilterQueryMixin, [
  //
  DynamoFilterQueryOperation,
  DynamoQueryScanProcessor,
]);

export default DynamoFilterQueryMixin;
