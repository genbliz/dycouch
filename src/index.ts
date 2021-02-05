export type {
  IFuseQueryIndexOptions,
  IFuseQueryParamOptions,
  IFuseIndexDefinition,
  IFuseFieldCondition,
  IFuseKeyConditionParams,
  IFusePagingParams,
  IFuseQueryConditionParams,
  IFusePagingResult,
  IFuseQueryDefinition,
} from "./type/types";

export { IFuseCoreEntityModel } from "./core/base-schema";
export { FuseGenericError } from "./helpers/errors";
export { DynamoDataOperation } from "./dynamo/dynamo-data-operation";
export { CouchDataOperation } from "./couch/couch-data-operation";
export { FuseInitializerDynamo } from "./dynamo/dynamo-initializer";
export { FuseInitializerCouch } from "./couch/couch-initializer";

// export const FuseDataOperationDynamo = DynamoDataOp;
// export const FuseDataOperationCouch = CouchDataOp;
// export default DynamoDataOp;
