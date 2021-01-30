export type {
  IDynamoQuerySecondayIndexOptions as IFuseQuerySecondayIndexOptions,
  IDynamoQueryParamOptions as IFuseQueryParamOptions,
  ISecondaryIndexDef as IFuseSecondaryIndexDef,
  IFieldCondition as IFuseFieldCondition,
  IDynamoKeyConditionParams as IFuseKeyConditionParams,
  IDynamoPagingParams as IFusePagingParams,
  IDynamoQueryConditionParams as IFuseQueryConditionParams,
  IDynamoPagingResult as IFusePagingResult,
  IQueryDefinition as IFuseQueryDefinition,
} from "./types";

export { IDynamoDataCoreEntityModel as IFuseDataCoreEntityModel } from "./core/base-schema";
export { GenericDataError, GenericFriendlyError } from "./helpers/errors";
import DynamoDataOp from "./dynamo/dynamo-data-operation";
export { DynamoInitializer as FuseInitializerDynamo } from "./dynamo/dynamo-initializer";
export { DynamoInitializer as FuseInitializerCouch } from "./dynamo/dynamo-initializer";

export const FuseDataOperationDynamo = DynamoDataOp;
export const FuseDataOperationCouch = DynamoDataOp;
export default DynamoDataOp;
