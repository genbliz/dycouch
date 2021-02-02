import {
  IFieldCondition,
  IDynamoQueryParamOptions,
  IDynamoPagingResult,
  IDynamoQuerySecondayIndexOptions,
} from "../types/types";

export abstract class RepoModel<T> {
  protected abstract fuse_getManyByIds({
    dataIds,
    fields,
    withCondition,
  }: {
    dataIds: string[];
    fields?: (keyof T)[];
    withCondition?: IFieldCondition<T>;
  }): Promise<T[]>;

  protected abstract fuse_createOne({ data }: { data: T }): Promise<T>;

  protected abstract fuse_deleteById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IFieldCondition<T>;
  }): Promise<T>;

  protected abstract fuse_getManyByCondition(paramOptions: IDynamoQueryParamOptions<T>): Promise<T[]>;

  protected abstract fuse_getManyByConditionPaginate(
    paramOptions: IDynamoQueryParamOptions<T>,
  ): Promise<IDynamoPagingResult<T[]>>;

  protected abstract fuse_getManyByIndex<TData = T, TSortKeyField = string>(
    paramOption: IDynamoQuerySecondayIndexOptions<TData, TSortKeyField>,
  ): Promise<T[]>;

  protected abstract fuse_getManyByIndexPaginate<TData = T, TSortKeyField = string>(
    paramOption: IDynamoQuerySecondayIndexOptions<TData, TSortKeyField>,
  ): Promise<IDynamoPagingResult<T[]>>;

  protected abstract fuse_getOneById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IFieldCondition<T>;
  }): Promise<T | null>;

  protected abstract fuse_updateOneById({
    dataId,
    data,
    withCondition,
  }: {
    dataId: string;
    data: T;
    withCondition?: IFieldCondition<T>;
  }): Promise<T>;

  protected abstract fuse_updateOneDirect({ data }: { data: T }): Promise<T>;
}
