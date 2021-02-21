type RequireAtLeastOneBase<T, Keys extends keyof T = keyof T> = Pick<T, Exclude<keyof T, Keys>> &
  {
    /* https://stackoverflow.com/questions/40510611/typescript-interface-require-one-of-two-properties-to-exist*/
    [K in Keys]-?: Required<Pick<T, K>> & Partial<Pick<T, Exclude<Keys, K>>>;
  }[Keys];
type RequireAtLeastOne<T> = RequireAtLeastOneBase<T, keyof T>;

type TypeFallBackStringOnly<T> = string extends T ? string : T;
type TypeFallBack<T> = undefined extends T ? Exclude<T, undefined> : T;
// type TypeFallBackStringOnly<T> = undefined extends T ? Exclude<T, undefined> : string extends T ? string : T;
type TypeFallBackArray<T> = number extends T ? number[] : string extends T ? string[] : T;

export type IFuseKeyConditionParams<T = any> = {
  // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LegacyConditionalParameters.KeyConditions.html
  $eq?: TypeFallBack<T>;
  $gt?: TypeFallBack<T>;
  $gte?: TypeFallBack<T>;
  $lt?: TypeFallBack<T>;
  $lte?: TypeFallBack<T>;
  $between?: [TypeFallBack<T>, TypeFallBack<T>];
  $beginsWith?: TypeFallBackStringOnly<T>;
};

export type IFuseQueryConditionParams<T = any> = IFuseKeyConditionParams<T> & {
  $ne?: TypeFallBack<T>;
  $in?: TypeFallBackArray<T>;
  $nin?: TypeFallBackArray<T>;
  $contains?: TypeFallBackStringOnly<T>;
  $notContains?: TypeFallBackStringOnly<T>;
  $exists?: boolean;
  $not?: IFuseKeyConditionParams<T>;
};

type QueryPartialAll<T> = {
  [P in keyof T]: T[P] | IFuseQueryConditionParams<T[P]>;
};

type QueryKeyConditionBasic<T> = {
  [P in keyof T]: T[P] | IFuseKeyConditionParams<T[P]>;
};

export interface IFusePagingResult<T> {
  lastKeyHash?: any;
  mainResult: T;
}

export type IFusePagingParams = {
  evaluationLimit?: number;
  pageSize?: number;
  lastKeyHash?: any;
  orderDesc?: boolean;
};

type IQueryDefOr<T> = { $or?: QueryPartialAll<RequireAtLeastOne<T>>[] };
type IQueryDefAnd<T> = { $and?: QueryPartialAll<RequireAtLeastOne<T>>[] };

export type IFuseQueryDefinition<T> = QueryPartialAll<RequireAtLeastOne<T & IQueryDefOr<T> & IQueryDefAnd<T>>>;

export interface IFuseQueryParamOptions<T, ISortKeyObjField = any> {
  query?: IFuseQueryDefinition<T>;
  fields?: (keyof T)[];
  partitionKeyQuery: { equals: string | number };
  sortKeyQuery?: QueryKeyConditionBasic<Required<ISortKeyObjField>>;
  pagingParams?: IFusePagingParams;
}

export interface IFuseQueryIndexOptions<T, TSortKeyField = string> {
  indexName: string;
  partitionKeyQuery: { equals: string | number };
  sortKeyQuery?: IFuseKeyConditionParams<TSortKeyField>;
  query?: IFuseQueryDefinition<T>;
  fields?: (keyof T)[];
  pagingParams?: IFusePagingParams;
}

export interface IFuseIndexDefinition<T> {
  indexName: string;
  partitionKeyFieldName: keyof T;
  sortKeyFieldName: keyof T;
  dataType: "N" | "S";
  projectionFieldsInclude?: (keyof T)[];
}

export type IFuseFieldCondition<T> = { field: keyof T; equals: string | number }[];
