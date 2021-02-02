import type {
  IFuseFieldCondition,
  IFuseIndexDefinition,
  IFusePagingResult,
  IFuseQueryIndexOptions,
  IFuseQueryParamOptions,
} from "../type/types";
import { RepoModel } from "../model/repo-model";
import Joi from "joi";
import { FuseInitializerCouch } from "./couch-initializer";
import { coreSchemaDefinition, IFuseCoreEntityModel } from "../core/base-schema";
import { FuseErrorUtils, GenericDataError } from "src/helpers/errors";
import { getJoiValidationErrors } from "src/helpers/base-joi-helper";
import { CouchFilterQueryOperation } from "./couch-filter-query-operation";

interface IDynamoOptions<T> {
  schemaDef: Joi.SchemaMap;
  couchDb: () => FuseInitializerCouch;
  dataKeyGenerator: () => string;
  featureEntityValue: string;
  secondaryIndexOptions: IFuseIndexDefinition<T>[];
  baseTableName: string;
  strictRequiredFields: (keyof T)[] | string[];
}

type IModelBase = IFuseCoreEntityModel;

type IFullEntity<T> = IFuseCoreEntityModel & T;

export default class CouchDataOperation<T> extends RepoModel<T> implements RepoModel<T> {
  private readonly _fuse_partitionKeyFieldName: keyof Pick<IModelBase, "id"> = "id";
  private readonly _fuse_sortKeyFieldName: keyof Pick<IModelBase, "featureEntity"> = "featureEntity";
  //
  private readonly _fuse_operationNotSuccessful = "Operation Not Successful";
  private readonly _fuse_entityResultFieldKeysMap: Map<string, string>;
  private readonly _fuse_couchDb: () => FuseInitializerCouch;
  private readonly _fuse_dataKeyGenerator: () => string;
  private readonly _fuse_schema: Joi.Schema;
  private readonly _fuse_tableFullName: string;
  private readonly _fuse_strictRequiredFields: string[];
  private readonly _fuse_featureEntityValue: string;
  private readonly _fuse_secondaryIndexOptions: IFuseIndexDefinition<T>[];
  private readonly _fuse_errorHelper: FuseErrorUtils;
  private readonly _fuse_filterQueryOperation = new CouchFilterQueryOperation();

  constructor({
    schemaDef,
    couchDb,
    secondaryIndexOptions,
    featureEntityValue,
    baseTableName,
    strictRequiredFields,
    dataKeyGenerator,
  }: IDynamoOptions<T>) {
    super();
    this._fuse_couchDb = couchDb;
    this._fuse_dataKeyGenerator = dataKeyGenerator;
    this._fuse_tableFullName = baseTableName;
    this._fuse_featureEntityValue = featureEntityValue;
    this._fuse_secondaryIndexOptions = secondaryIndexOptions;
    this._fuse_strictRequiredFields = strictRequiredFields as string[];
    this._fuse_errorHelper = new FuseErrorUtils();
    this._fuse_entityResultFieldKeysMap = new Map();

    const fullSchemaMapDef = { ...schemaDef, ...coreSchemaDefinition };

    Object.keys(fullSchemaMapDef).forEach((key) => {
      this._fuse_entityResultFieldKeysMap.set(key, key);
    });

    this._fuse_schema = Joi.object().keys({
      ...fullSchemaMapDef,
      _id: Joi.string().required().min(5).max(1500),
    });
  }

  private _fuse_generateDynamoTableKey() {
    return this._fuse_dataKeyGenerator();
  }

  private _fuse_couchDbInstance() {
    return this._fuse_couchDb().getInstance();
  }

  // export type IFullEntity<T> = ICoreEntityBaseModel & T;

  private _fuse_getLocalVariables() {
    return {
      partitionKeyFieldName: this._fuse_partitionKeyFieldName,
      sortKeyFieldName: this._fuse_sortKeyFieldName,
      //
      featureEntityValue: this._fuse_featureEntityValue,
      //
      tableFullName: this._fuse_tableFullName,
      secondaryIndexOptions: this._fuse_secondaryIndexOptions,
      strictRequiredFields: this._fuse_strictRequiredFields,
    } as const;
  }

  private _fuse_stripNonRequiredOutputData({ dataObj, excludeFields }: { dataObj: any; excludeFields?: string[] }): T {
    const returnData = {} as any;
    if (typeof dataObj === "object" && this._fuse_entityResultFieldKeysMap.size > 0) {
      Object.entries(dataObj).forEach(([key, value]) => {
        if (this._fuse_entityResultFieldKeysMap.has(key)) {
          if (excludeFields?.length) {
            if (!excludeFields.includes(key)) {
              returnData[key] = value;
            }
          } else {
            returnData[key] = value;
          }
        }
      });
    }
    return returnData;
  }

  private _fuse_getNativePouchId(dataId: string) {
    const { featureEntityValue } = this._fuse_getLocalVariables();
    return [featureEntityValue, dataId].join("#");
  }

  private _fuse_getBaseObject({ dataId }: { dataId: string }) {
    const { partitionKeyFieldName, sortKeyFieldName, featureEntityValue } = this._fuse_getLocalVariables();

    const dataMust = {
      _id: this._fuse_getNativePouchId(dataId),
      [partitionKeyFieldName]: dataId,
      [sortKeyFieldName]: featureEntityValue,
    };
    return dataMust;
  }

  private _fuse_withConditionPassed({ item, withCondition }: { item: any; withCondition?: IFuseFieldCondition<T> }) {
    if (item && typeof item === "object" && withCondition?.length) {
      const isPassed = withCondition.every(({ field, equals }) => {
        return item[field] !== undefined && item[field] === equals;
      });
      return isPassed;
    }
    return true;
  }

  private _fuse_checkValidateMustBeAnObjectDataType(data: unknown) {
    if (!data || typeof data !== "object") {
      throw this._fuse_createGenericError(`Data MUST be valid object`);
    }
  }

  private _fuse_checkValidateStrictRequiredFields(onDataObj: any) {
    this._fuse_checkValidateMustBeAnObjectDataType(onDataObj);

    const { strictRequiredFields } = this._fuse_getLocalVariables();

    if (strictRequiredFields?.length) {
      for (const field of strictRequiredFields) {
        if (onDataObj[field] === null || onDataObj[field] === undefined) {
          throw this._fuse_createGenericError(`Strict required field NOT defined`);
        }
      }
    }
  }

  private _fuse_removeDuplicateString(list: string[]) {
    return Array.from(new Set(list));
  }

  private async _fuse_allHelpValidateGetValue(data: any) {
    const { error, value } = this._fuse_schema.validate(data, {
      stripUnknown: true,
    });

    if (error) {
      const msg = getJoiValidationErrors(error) ?? "Validation error occured";
      throw this._fuse_errorHelper.fuse_helper_createFriendlyError(msg);
    }

    return await Promise.resolve({
      validatedData: value,
    });
  }

  private _fuse_createGenericError(error: string) {
    return new GenericDataError(error);
  }

  protected async fuse_createOne({ data }: { data: T }): Promise<T> {
    this._fuse_checkValidateStrictRequiredFields(data);

    const { partitionKeyFieldName } = this._fuse_getLocalVariables();

    let dataId: string | undefined = data[partitionKeyFieldName];

    if (!dataId) {
      dataId = this._fuse_generateDynamoTableKey();
    }

    const dataMust = this._fuse_getBaseObject({ dataId });
    const fullData = { ...data, ...dataMust };

    const validated = await this._fuse_allHelpValidateGetValue(fullData);

    const result = await this._fuse_couchDbInstance().put(validated.validatedData);
    if (!result.ok) {
      throw this._fuse_createGenericError(this._fuse_operationNotSuccessful);
    }
    return this._fuse_stripNonRequiredOutputData({ dataObj: data });
  }

  protected async fuse_getManyByIds({
    dataIds,
    fields,
    withCondition,
  }: {
    dataIds: string[];
    fields?: (keyof T)[] | undefined;
    withCondition?: IFuseFieldCondition<T> | undefined;
  }): Promise<T[]> {
    //
    const uniqueIds = this._fuse_removeDuplicateString(dataIds);
    const fullUniqueIds = uniqueIds.map((id) => this._fuse_getNativePouchId(id));

    const data = await this._fuse_couchDbInstance().allDocs<IFullEntity<T>>({
      keys: fullUniqueIds,
      include_docs: true,
    });

    const dataList: T[] = [];

    if (withCondition?.length) {
      data?.rows?.forEach((item) => {
        if (item?.doc?.featureEntity === this._fuse_featureEntityValue) {
          const passed = this._fuse_withConditionPassed({ item: item.doc, withCondition });
          if (passed) {
            const k = this._fuse_stripNonRequiredOutputData({
              dataObj: item.doc,
              excludeFields: fields as any[],
            });
            dataList.push(k);
          }
        }
      });
    } else {
      data?.rows?.forEach((item) => {
        if (item?.doc?.featureEntity === this._fuse_featureEntityValue) {
          const k = this._fuse_stripNonRequiredOutputData({
            dataObj: item.doc,
            excludeFields: fields as any[],
          });
          dataList.push(k);
        }
      });
    }
    return dataList;
  }

  protected async fuse_getManyByCondition(paramOptions: IFuseQueryParamOptions<T, any>): Promise<T[]> {
    if (!paramOptions?.query) {
      throw this._fuse_createGenericError("Invalid query object");
    }

    paramOptions.query = {
      ...paramOptions.query,
      featureEntity: this._fuse_featureEntityValue,
    };

    const _normalizeFields = (fields: (keyof T)[] | string[]) => {
      if (fields?.length) {
        const fieldList: string[] = [];
        for (const field of fields as string[]) {
          fieldList.push(field);
        }
        if (fieldList.length) {
          return this._fuse_removeDuplicateString(fieldList);
        }
      }
      return undefined;
    };

    const queryDefData = this._fuse_filterQueryOperation.processQueryFilter({
      queryDefs: paramOptions.query,
    });

    const data = await this._fuse_couchDbInstance().find({
      selector: { ...queryDefData },
      fields: _normalizeFields(paramOptions?.fields || []),
    });
    const dataList = data?.docs?.map((item) => {
      return this._fuse_stripNonRequiredOutputData({ dataObj: item });
    });
    return dataList || [];
  }

  async fuse_getAll(): Promise<T[]> {
    const data = await this._fuse_couchDbInstance().allDocs<IFullEntity<T>>({
      include_docs: true,
      startkey: this._fuse_featureEntityValue,
      endkey: `${this._fuse_featureEntityValue}\ufff0`,
    });
    const dataList: T[] = [];
    data?.rows?.forEach((item) => {
      if (item?.doc?.featureEntity === this._fuse_featureEntityValue) {
        const k = this._fuse_stripNonRequiredOutputData({ dataObj: item.doc });
        dataList.push(k);
      }
    });
    return dataList;
  }

  protected fuse_getManyByConditionPaginate(
    paramOptions: IFuseQueryParamOptions<T, any>,
  ): Promise<IFusePagingResult<T[]>> {
    throw new Error("Method not implemented.");
  }

  protected fuse_getManyByIndex<TData = T, TSortKeyField = string>(
    paramOption: IFuseQueryIndexOptions<TData, TSortKeyField>,
  ): Promise<T[]> {
    throw new Error("Method not implemented.");
  }

  protected fuse_getManyByIndexPaginate<TData = T, TSortKeyField = string>(
    paramOption: IFuseQueryIndexOptions<TData, TSortKeyField>,
  ): Promise<IFusePagingResult<T[]>> {
    throw new Error("Method not implemented.");
  }

  // protected async fuse_getManyByIndexPaginate00<TData = T, TSortKeyField = string>(
  //   paramOption: IFuseQuerySecondayIndexOptions<TData, TSortKeyField>,
  // ): Promise<IFusePagingResult<T[]>> {
  //   if (!paramOption?.query) {
  //     throw new GenericDataError("Invalid query object");
  //   }
  //   if (paramOption.indexName) {
  //     throw new GenericDataError("Index MUST be defined for sort query");
  //   }

  //   paramOption.query = {
  //     ...paramOption.query,
  //     featureEntity: this.here_featureEntityValue,
  //   };

  //   const _normalizeFields = (fields: (keyof T)[] | string[]) => {
  //     if (fields?.length) {
  //       const fieldList: string[] = [];
  //       for (const field of fields as string[]) {
  //         if (field === "id") {
  //           fieldList.push("_id");
  //         } else {
  //           fieldList.push(field);
  //         }
  //       }
  //       if (fieldList.length) {
  //         return this._root_removeDuplicateString(fieldList);
  //       }
  //     }
  //     return undefined;
  //   };

  //   let paramsSort: Array<string | { [propName: string]: "asc" | "desc" }> | undefined = undefined;

  //   const indexOpt = this._indexOptions.find((f) => f.indexName === params.indexName);
  //   if (indexOpt?.fields?.length) {
  //     const query00: any = { ...paramOption.query };
  //     const query01: any = {};
  //     const query02: any = {};
  //     for (const indexOrder of indexOpt.fields) {
  //       if (query00[indexOrder] !== undefined) {
  //         query01[indexOrder] = query00[indexOrder];
  //       }
  //     }
  //     Object.entries(query00).forEach(([key, val]) => {
  //       if (query01[key] === undefined) {
  //         query02[key] = val;
  //       }
  //     });
  //     console.log({ query01, query02 });
  //     paramOption.query = { ...query01, ...query02 };
  //     //
  //     if (paramOption.pagingParams?.orderDesc) {
  //       if (Array.isArray(params.sort)) {
  //         if (params.sort.length) {
  //           paramsSort = [...params.sort] as any[];
  //         }
  //       } else {
  //         const sort00 = [...params.sort?.advanced];
  //         const sort01: Array<{ [propName: string]: "asc" | "desc" }> = [];

  //         for (const fieldName of indexOpt.fields) {
  //           const sortFind = sort00.find((f) => f.field === fieldName);
  //           if (sortFind && typeof sortFind === "object" && sortFind.field) {
  //             sort01.push({ [sortFind.field]: sortFind.sortType });
  //           }
  //         }
  //         if (sort01.length) {
  //           paramsSort = [...sort01];
  //         }
  //         console.log({ sort01 });
  //       }
  //     }
  //   }

  //   console.log({ indexName: params.indexName });
  //   const data = await this._couchDbInstance().find({
  //     selector: { ...paramOption.query },
  //     fields: _normalizeFields(paramOption.fields),
  //     limit: params.limit || undefined,
  //     skip: params.skip || undefined,
  //     sort: paramsSort,
  //     use_index: params.indexName ? params.indexName : undefined,
  //   });
  //   const dataList = data?.docs?.map((item) => {
  //     return this._root_stripNonRequiredData(item);
  //   });
  //   return dataList;
  // }

  protected async fuse_getOneById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IFuseFieldCondition<T> | undefined;
  }): Promise<T | null> {
    this._fuse_errorHelper.fuse_helper_validateRequiredString({ dataId });

    const nativeId = this._fuse_getNativePouchId(dataId);

    const dataInDb = await this._fuse_couchDbInstance().get<IFullEntity<T>>(nativeId);
    if (!(dataInDb?.id === dataId && dataInDb.featureEntity === this._fuse_featureEntityValue)) {
      return null;
    }
    const passed = this._fuse_withConditionPassed({ item: dataInDb, withCondition });
    if (!passed) {
      return null;
    }
    return this._fuse_stripNonRequiredOutputData({ dataObj: dataInDb });
  }

  protected async fuse_updateOneById({
    dataId,
    data,
    withCondition,
  }: {
    dataId: string;
    data: T;
    withCondition?: IFuseFieldCondition<T> | undefined;
  }): Promise<T> {
    this._fuse_errorHelper.fuse_helper_validateRequiredString({ dataId });

    const nativeId = this._fuse_getNativePouchId(dataId);

    const dataInDb = await this._fuse_couchDbInstance().get<IFullEntity<T>>(nativeId);
    if (!(dataInDb?.id === dataId && dataInDb.featureEntity === this._fuse_featureEntityValue && dataInDb._rev)) {
      throw this._fuse_createGenericError("Record does not exists");
    }
    const passed = this._fuse_withConditionPassed({ item: dataInDb, withCondition });
    if (!passed) {
      throw this._fuse_createGenericError("Record with conditions does not exists");
    }
    const _data: IFullEntity<T> = { ...data } as any;

    const validated = await this._fuse_allHelpValidateGetValue(_data);

    const result = await this._fuse_couchDbInstance().put<T>({
      ...validated.validatedData,
      _rev: dataInDb._rev,
    });
    if (!result.ok) {
      throw this._fuse_createGenericError(this._fuse_operationNotSuccessful);
    }
    return this._fuse_stripNonRequiredOutputData({ dataObj: data });
  }

  protected fuse_updateOneDirect({ data }: { data: T }): Promise<T> {
    throw new Error("Method not implemented.");
  }

  protected async fuse_deleteById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IFuseFieldCondition<T> | undefined;
  }): Promise<T> {
    const nativeId = this._fuse_getNativePouchId(dataId);
    const dataInDb = await this._fuse_couchDbInstance().get<IFullEntity<T>>(nativeId);

    if (!(dataInDb?._id === dataId && dataInDb.featureEntity === this._fuse_featureEntityValue)) {
      throw this._fuse_createGenericError("Record does not exists");
    }
    const passed = this._fuse_withConditionPassed({ item: dataInDb, withCondition });
    if (!passed) {
      throw this._fuse_createGenericError("Record with conditions does not exists for deletion");
    }
    const result = await this._fuse_couchDbInstance().remove(dataInDb);
    if (!result.ok) {
      throw this._fuse_createGenericError(this._fuse_operationNotSuccessful);
    }
    return this._fuse_stripNonRequiredOutputData({ dataObj: dataInDb });
  }
}
