// import { LoggingService } from "../helpers/logging-service";
// import { UtilService } from "../helpers/util-service";
import type { IDynamoQueryConditionParams, IQueryDefinition } from "../types";

export type IConditionOperators = PouchDB.Find.ConditionOperators;
export type ICombinationOperators = PouchDB.Find.CombinationOperators;

interface IQueryConditionsKeys {
  // $and?: Selector[];
  // $or?: Selector[];
  $lt?: any;
  $gt?: any;
  $lte?: any;
  $gte?: any;
  $eq?: any;
  $ne?: any;
  $exists?: boolean;
  $in?: any[];
  $nin?: any[];
  $regex?: string;
}

type FieldPartial<T> = { [P in keyof T]-?: any };
const conditionKeyMap: FieldPartial<IDynamoQueryConditionParams> = {
  $eq: "$eq",
  $notEq: "$ne",
  $lt: "$lt",
  $lte: "$lte",
  $gt: "$gt",
  $gte: "$gte",
  $exists: "",
  $notExists: "",
  $in: "",
  $between: "",
  $contains: "",
  $notContains: "",
  $beginsWith: "",
};

// type IDictionaryAttr = { [key: string]: any };

type FieldPartialQuery<T> = { [P in keyof T]-?: T[P] };
type IQueryConditions = {
  [fieldName: string]: FieldPartialQuery<IQueryConditionsKeys>;
};

function hasQueryConditionKey(key: string) {
  return Object.keys(conditionKeyMap).includes(key);
}

export class CouchFilterQueryOperation {
  private operation__filterFieldExist({ fieldName }: { fieldName: string }): IQueryConditions {
    const result = {
      [fieldName]: { $exists: true },
    } as IQueryConditions;
    return result;
  }

  private operation__filterFieldNotExist({ fieldName }: { fieldName: string }): IQueryConditions {
    const result = {
      [fieldName]: { $exists: false },
    } as IQueryConditions;
    return result;
  }

  fuse__helperFilterBasic({
    fieldName,
    val,
    conditionExpr,
  }: {
    fieldName: string;
    conditionExpr?: string;
    val: string | number;
  }): IQueryConditions {
    if (conditionExpr) {
      return {
        [fieldName]: { [conditionExpr]: val },
      } as any;
    }
    const result = {
      [fieldName]: { $eq: val },
    } as IQueryConditions;
    return result;
  }

  private operation__filterIn({ fieldName, attrValues }: { fieldName: string; attrValues: any[] }): IQueryConditions {
    const result = {
      [fieldName]: { $in: attrValues },
    } as IQueryConditions;
    return result;
  }

  private operation__filterContains({ fieldName, term }: { fieldName: string; term: any }): IQueryConditions {
    const re = new RegExp(`^${term}`);
    const result = {
      [fieldName]: { $regex: re.source },
    } as IQueryConditions;
    return result;
  }

  private operation__filterBetween({
    fieldName,
    from,
    to,
  }: {
    fieldName: string;
    from: any;
    to: any;
  }): IQueryConditions {
    const result = {
      [fieldName]: { $gte: from, $lte: to },
    } as IQueryConditions;
    return result;
  }

  private operation__filterBeginsWith({ fieldName, term }: { fieldName: string; term: any }): IQueryConditions {
    const result = {
      [fieldName]: { $regex: `^${term}` },
    } as IQueryConditions;
    return result;
  }

  private operation__translateAdvancedQueryOperation({
    fieldName,
    queryObject,
  }: {
    fieldName: string;
    queryObject: any;
  }) {
    const queryConditions: IQueryConditions[] = [];
    Object.keys(queryObject).forEach((condKey) => {
      const conditionKey = condKey as keyof IDynamoQueryConditionParams;
      const _conditionObjValue = queryObject[conditionKey];
      if (conditionKey === "$between") {
        if (Array.isArray(_conditionObjValue)) {
          const _queryConditions = this.operation__filterBetween({
            fieldName: fieldName,
            from: _conditionObjValue[0],
            to: _conditionObjValue[1],
          });
          queryConditions.push(_queryConditions);
        }
      } else if (conditionKey === "$beginsWith") {
        const _queryConditions = this.operation__filterBeginsWith({
          fieldName: fieldName,
          term: _conditionObjValue,
        });
        queryConditions.push(_queryConditions);
      } else if (conditionKey === "$contains") {
        const _queryConditions = this.operation__filterContains({
          fieldName: fieldName,
          term: _conditionObjValue,
        });
        queryConditions.push(_queryConditions);
      } else if (conditionKey === "$in") {
        if (Array.isArray(_conditionObjValue)) {
          const _queryConditions = this.operation__filterIn({
            fieldName: fieldName,
            attrValues: _conditionObjValue,
          });
          queryConditions.push(_queryConditions);
        }
        // filterFieldNotExist({ fieldName, termValue }
      } else if (conditionKey === "$notContains") {
        // const _queryConditions = this.operation__filterContains({
        //   fieldName: fieldName,
        //   term: _conditionObjValue,
        // });
        // _queryConditions.xFilterExpression = `NOT ${_queryConditions.xFilterExpression}`;
        // queryConditions.push(_queryConditions);
      } else if (conditionKey === "$exists") {
        const _queryConditions = this.operation__filterFieldExist({
          fieldName: fieldName,
        });
        queryConditions.push(_queryConditions);
      } else if (conditionKey === "$notExists") {
        const _queryConditions = this.operation__filterFieldNotExist({
          fieldName: fieldName,
        });
        queryConditions.push(_queryConditions);
      } else {
        if (hasQueryConditionKey(conditionKey)) {
          const conditionExpr = conditionKeyMap[conditionKey];
          if (conditionExpr) {
            const _queryConditions = this.fuse__helperFilterBasic({
              fieldName: fieldName,
              val: _conditionObjValue,
              conditionExpr: conditionExpr,
            });
            queryConditions.push(_queryConditions);
          }
        }
      }
    });
    return queryConditions;
  }

  private operation_translateBasicQueryOperation({ fieldName, queryObject }: { fieldName: string; queryObject: any }) {
    const _queryConditions = this.fuse__helperFilterBasic({
      fieldName: fieldName,
      val: queryObject,
      // conditionExpr: "$eq",
    });
    return _queryConditions;
  }

  fuse__helperDynamoFilterOperation({ queryDefs }: { queryDefs: IQueryDefinition<any>["query"] }) {
    let queryMainConditions: IQueryConditions[] = [];
    let queryAndConditions: IQueryConditions[] = [];
    let queryOrConditions: IQueryConditions[] = [];

    Object.keys(queryDefs).forEach((fieldName_Or_And) => {
      if (fieldName_Or_And === "$or") {
        const orKey = fieldName_Or_And;
        const orArray: any[] = queryDefs[orKey];
        if (Array.isArray(orArray)) {
          orArray.forEach((orQuery) => {
            Object.keys(orQuery).forEach((fieldName) => {
              //
              const orQueryObjectOrValue = orQuery[fieldName];
              //
              if (typeof orQueryObjectOrValue === "object") {
                const _orQueryCond = this.operation__translateAdvancedQueryOperation({
                  fieldName,
                  queryObject: orQueryObjectOrValue,
                });
                queryOrConditions = [...queryOrConditions, ..._orQueryCond];
              } else {
                const _orQueryConditions = this.operation_translateBasicQueryOperation({
                  fieldName,
                  queryObject: orQueryObjectOrValue,
                });
                queryOrConditions = [...queryOrConditions, _orQueryConditions];
              }
            });
          });
        }
      } else if (fieldName_Or_And === "$and") {
        const andKey = fieldName_Or_And;
        const andArray: any[] = queryDefs[andKey];
        if (Array.isArray(andArray)) {
          andArray.forEach((andQuery) => {
            Object.keys(andQuery).forEach((fieldName) => {
              //
              const andQueryObjectOrValue = andQuery[fieldName];
              //
              if (typeof andQueryObjectOrValue === "object") {
                const _andQueryCond = this.operation__translateAdvancedQueryOperation({
                  fieldName,
                  queryObject: andQueryObjectOrValue,
                });
                queryAndConditions = [...queryAndConditions, ..._andQueryCond];
              } else {
                const _andQueryConditions = this.operation_translateBasicQueryOperation({
                  fieldName,
                  queryObject: andQueryObjectOrValue,
                });
                queryAndConditions = [...queryAndConditions, _andQueryConditions];
              }
            });
          });
        }
      } else {
        const fieldName2 = fieldName_Or_And;
        const queryObjectOrValue = queryDefs[fieldName2];
        if (typeof queryObjectOrValue === "object") {
          const _queryCond = this.operation__translateAdvancedQueryOperation({
            fieldName: fieldName2,
            queryObject: queryObjectOrValue,
          });
          queryMainConditions = [...queryMainConditions, ..._queryCond];
        } else {
          const _queryConditions = this.operation_translateBasicQueryOperation({
            fieldName: fieldName2,
            queryObject: queryObjectOrValue,
          });
          queryMainConditions = [...queryMainConditions, _queryConditions];
        }
      }
    });

    let queryAllConditions: IQueryConditions & { $and: IQueryConditions[] } & { $or: IQueryConditions[] } = {} as any;

    for (const item1 of queryMainConditions) {
      queryAllConditions = { ...queryAllConditions, ...item1 };
    }

    if (queryAndConditions.length) {
      queryAllConditions.$and = queryAndConditions;
    }

    if (queryOrConditions.length) {
      queryAllConditions.$or = queryOrConditions;
    }
    return queryAllConditions;
  }
}

const dd = new CouchFilterQueryOperation();

const _searchTerm = "chuks";

const query: any = {
  amount: { $gt: 0 },
  category: { $gt: "Nunu" },
  $or: [
    { amount: { $contains: _searchTerm } },
    { category: { $contains: _searchTerm } },
    { invoiceId: { $contains: _searchTerm } },
    { transactionId: { $contains: _searchTerm } },
    { remark: { $contains: _searchTerm } },
  ],
  $and: [
    //
    { category: _searchTerm },
    { invoiceId: { $eq: _searchTerm } },
  ],
};

const result = dd.fuse__helperDynamoFilterOperation({ queryDefs: query });

console.log(JSON.stringify(result, null, 2));
process.exit(0);
