import { LoggingService } from "../helpers/logging-service";
import { UtilService } from "../helpers/util-service";
import type { IFuseQueryConditionParams, IFuseQueryDefinition } from "../type/types";

type FieldPartial<T> = { [P in keyof T]-?: any };
const conditionKeyMap: FieldPartial<IFuseQueryConditionParams> = {
  $eq: "=",
  $notEq: "<>",
  $lt: "<",
  $lte: "<=",
  $gt: ">",
  $gte: ">=",
  $exists: "",
  $notExists: "",
  $in: "",
  $between: "",
  $contains: "",
  $notContains: "",
  $beginsWith: "",
};

type IDictionaryAttr = { [key: string]: any };
type IQueryConditions = {
  xExpressionAttributeValues: IDictionaryAttr;
  xExpressionAttributeNames: IDictionaryAttr;
  xFilterExpression: string;
};

function hasQueryConditionKey(key: string) {
  const queryCondKeys = Object.keys(conditionKeyMap);
  return queryCondKeys.includes(key);
}

const getRandom = () =>
  [Math.round(Math.random() * 99999), Math.round(Math.random() * 88888), Math.round(Math.random() * 99)].join("");

export class DynamoFilterQueryOperation {
  private operation__filterFieldExist({ fieldName }: { fieldName: string }): IQueryConditions {
    const attrKeyHash = `#attrKey1${getRandom()}`.toLowerCase();
    const result = {
      xExpressionAttributeNames: {
        [attrKeyHash]: fieldName,
      },
      xFilterExpression: `attribute_exists (${attrKeyHash})`,
    } as IQueryConditions;
    return result;
  }

  private operation__filterFieldNotExist({ fieldName }: { fieldName: string }): IQueryConditions {
    const attrKeyHash = `#attrKey2${getRandom()}`.toLowerCase();
    const result = {
      xExpressionAttributeNames: {
        [attrKeyHash]: fieldName,
      },
      xFilterExpression: `attribute_not_exists (${attrKeyHash})`,
    } as IQueryConditions;
    return result;
  }

  fuse__helperFilterBasic({
    fieldName,
    val,
    conditionExpr,
  }: {
    fieldName: string;
    conditionExpr: string;
    val: string | number;
  }): IQueryConditions {
    const keyAttr = `:attr${fieldName}${getRandom()}`.toLowerCase();
    const attrKeyHash = `#attrKey3${getRandom()}`.toLowerCase();
    const result: IQueryConditions = {
      xExpressionAttributeValues: {
        [keyAttr]: val,
      },
      xExpressionAttributeNames: {
        [attrKeyHash]: fieldName,
      },
      xFilterExpression: [attrKeyHash, conditionExpr, keyAttr].join(" "),
    };
    return result;
  }

  private operation__filterIn({ fieldName, attrValues }: { fieldName: string; attrValues: any[] }): IQueryConditions {
    const expressAttrVal: { [key: string]: string } = {};
    const expressAttrName: { [key: string]: string } = {};
    const filterExpress: string[] = [];

    const _attrKeyHash = `#attrKey4${getRandom()}`.toLowerCase();
    expressAttrName[_attrKeyHash] = fieldName;

    attrValues.forEach((item) => {
      const keyAttr = `:attr${fieldName}${getRandom()}`.toLowerCase();
      expressAttrVal[keyAttr] = item;
      filterExpress.push(`${_attrKeyHash} = ${keyAttr}`);
    });

    const _filterExpression = filterExpress.join(" OR ").trim();
    const _filterExpressionValue = filterExpress.length > 1 ? `(${_filterExpression})` : _filterExpression;

    const result: IQueryConditions = {
      xExpressionAttributeValues: {
        ...expressAttrVal,
      },
      xExpressionAttributeNames: {
        ...expressAttrName,
      },
      xFilterExpression: _filterExpressionValue,
    };
    return result;
  }

  private operation__filterContains({ fieldName, term }: { fieldName: string; term: any }): IQueryConditions {
    const attrKeyHash = `#attrKey5${getRandom()}`.toLowerCase();
    const keyAttr = `:attr${fieldName}${getRandom()}`.toLowerCase();
    const result: IQueryConditions = {
      xExpressionAttributeValues: {
        [keyAttr]: term,
      },
      xExpressionAttributeNames: {
        [attrKeyHash]: fieldName,
      },
      xFilterExpression: `contains (${attrKeyHash}, ${keyAttr})`,
    };
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
    const _attrKeyHash = `#attrKey6${getRandom()}`.toLowerCase();
    const _fromKey = `:fromKey${getRandom()}`.toLowerCase();
    const _toKey = `:toKey${getRandom()}`.toLowerCase();
    const result: IQueryConditions = {
      xExpressionAttributeValues: {
        [_fromKey]: from,
        [_toKey]: to,
      },
      xExpressionAttributeNames: {
        [_attrKeyHash]: fieldName,
      },
      xFilterExpression: [_attrKeyHash, "between", _fromKey, "and", _toKey].join(" "),
    };
    return result;
  }

  private operation__filterBeginsWith({ fieldName, term }: { fieldName: string; term: any }): IQueryConditions {
    const _attrKeyHash = `#attrKey7${getRandom()}`.toLowerCase();
    const keyAttr = `:attr${fieldName}${getRandom()}`.toLowerCase();
    const result: IQueryConditions = {
      xExpressionAttributeValues: {
        [keyAttr]: term,
      },
      xExpressionAttributeNames: {
        [_attrKeyHash]: fieldName,
      },
      xFilterExpression: `begins_with (${_attrKeyHash}, ${keyAttr})`,
    };
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
      const conditionKey = condKey as keyof IFuseQueryConditionParams;
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
        const _queryConditions = this.operation__filterContains({
          fieldName: fieldName,
          term: _conditionObjValue,
        });
        _queryConditions.xFilterExpression = `NOT ${_queryConditions.xFilterExpression}`;
        queryConditions.push(_queryConditions);
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
      conditionExpr: "=",
    });
    return _queryConditions;
  }

  fuse__helperDynamoFilterOperation({
    queryDefs,
    projectionFields,
  }: {
    queryDefs: IFuseQueryDefinition<any>["query"];
    projectionFields: any[] | undefined | null;
  }) {
    let queryAndConditions: IQueryConditions[] = [];
    let queryOrConditions: IQueryConditions[] = [];
    //
    let _projectionExpression: string | undefined = undefined;
    const andFilterExpressionArray: string[] = [];
    const orFilterExpressionArray: string[] = [];
    let _expressionAttributeValues: IDictionaryAttr = {};
    let _expressionAttributeNames: IDictionaryAttr = {};

    Object.keys(queryDefs).forEach((fieldName_Or_And) => {
      if (fieldName_Or_And === "$or") {
        const orKey = fieldName_Or_And;
        const orArray: any[] = queryDefs[orKey];
        if (orArray && Array.isArray(orArray)) {
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
        if (andArray && Array.isArray(andArray)) {
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
        if (fieldName_Or_And) {
          const fieldName2 = fieldName_Or_And;
          const queryObjectOrValue = queryDefs[fieldName2];
          if (queryObjectOrValue) {
            if (typeof queryObjectOrValue === "object") {
              const _queryCond = this.operation__translateAdvancedQueryOperation({
                fieldName: fieldName2,
                queryObject: queryObjectOrValue,
              });
              queryAndConditions = [...queryAndConditions, ..._queryCond];
            } else {
              const _queryConditions = this.operation_translateBasicQueryOperation({
                fieldName: fieldName2,
                queryObject: queryObjectOrValue,
              });
              queryAndConditions = [...queryAndConditions, _queryConditions];
            }
          }
        }
      }
    });

    for (const item of queryAndConditions) {
      _expressionAttributeNames = {
        ..._expressionAttributeNames,
        ...item.xExpressionAttributeNames,
      };
      _expressionAttributeValues = {
        ..._expressionAttributeValues,
        ...item.xExpressionAttributeValues,
      };
      andFilterExpressionArray.push(item.xFilterExpression);
    }

    for (const item2 of queryOrConditions) {
      _expressionAttributeNames = {
        ..._expressionAttributeNames,
        ...item2.xExpressionAttributeNames,
      };
      _expressionAttributeValues = {
        ..._expressionAttributeValues,
        ...item2.xExpressionAttributeValues,
      };
      orFilterExpressionArray.push(item2.xFilterExpression);
    }

    let _andfilterExpression: string | null = null;
    let _orfilterExpression: string | null = null;

    if (andFilterExpressionArray?.length) {
      _andfilterExpression = andFilterExpressionArray.join(" AND ").trim();
    }

    if (orFilterExpressionArray?.length) {
      _orfilterExpression = orFilterExpressionArray.join(" OR ").trim();
    }

    let _filterExpression: string = "";

    if (_andfilterExpression && _orfilterExpression) {
      _filterExpression = `(${_andfilterExpression}) AND (${_orfilterExpression})`;
      //
    } else if (_andfilterExpression) {
      _filterExpression = _andfilterExpression;
      //
    } else if (_orfilterExpression) {
      _filterExpression = _orfilterExpression;
    }

    if (projectionFields?.length && Array.isArray(projectionFields)) {
      const _projection_expressionAttributeNames: IDictionaryAttr = {};
      projectionFields.forEach((field) => {
        if (typeof field === "string") {
          const attrKeyHash = `#attrKey8${getRandom()}`.toLowerCase();
          _projection_expressionAttributeNames[attrKeyHash] = field;
        }
      });
      _projectionExpression = Object.keys(_projection_expressionAttributeNames).join(", ");
      _expressionAttributeNames = {
        ..._projection_expressionAttributeNames,
        ..._expressionAttributeNames,
      };
    }

    const _expressionAttributeValuesFinal = UtilService.objectHasAnyProperty(_expressionAttributeValues)
      ? _expressionAttributeValues
      : undefined;
    //
    const _expressionAttributeNamesFinal = UtilService.objectHasAnyProperty(_expressionAttributeNames)
      ? _expressionAttributeNames
      : undefined;

    const queryExpressions = {
      expressionAttributeValues: _expressionAttributeValuesFinal,
      filterExpression: _filterExpression,
      projectionExpressionAttr: _projectionExpression,
      expressionAttributeNames: _expressionAttributeNamesFinal,
    };
    LoggingService.log({ queryExpressions });
    return queryExpressions;
  }
}
