export type {
  IDynamoQuerySecondayIndexOptions,
  IDynamoQueryParamOptions,
  ISecondaryIndexDef,
  IFieldCondition,
  IDynamoKeyConditionParams,
  IDynamoPagingParams,
  IDynamoQueryConditionParams,
  IDynamoPagingResult,
  IQueryDefinition,
} from "./types";
export { IDynamoDataCoreEntityModel } from "./core/base-schema";
export { GenericDataError, GenericFriendlyError } from "./helpers/errors";
import DynamoDataOp from "./dynamo/dynamo-data-operation";
export { DynamoInitializer } from "./dynamo/dynamo-initializer";

export const DynamoDataOperation = DynamoDataOp;
export default DynamoDataOp;
