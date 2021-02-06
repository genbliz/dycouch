import { LoggingService } from "./../helpers/logging-service";
import type { IFuseIndexDefinition } from "./../type/types";
import type { FuseInitializerCouch } from "./couch-initializer";

interface ITableOptions<T> {
  couchDb: () => FuseInitializerCouch;
  secondaryIndexOptions: IFuseIndexDefinition<T>[];
  tableFullName: string;
  partitionKeyFieldName: string;
  sortKeyFieldName: string;
}

export class CouchManageTable<T> {
  private readonly partitionKeyFieldName: string;
  private readonly sortKeyFieldName: string;
  private readonly couchDb: () => FuseInitializerCouch;
  private readonly tableFullName: string;
  private readonly secondaryIndexOptions: IFuseIndexDefinition<T>[];

  constructor({
    couchDb,
    secondaryIndexOptions,
    tableFullName,
    partitionKeyFieldName,
    sortKeyFieldName,
  }: ITableOptions<T>) {
    this.couchDb = couchDb;
    this.tableFullName = tableFullName;
    this.partitionKeyFieldName = partitionKeyFieldName;
    this.sortKeyFieldName = sortKeyFieldName;
    this.secondaryIndexOptions = secondaryIndexOptions;
    this._trickLinter();
  }

  private _trickLinter() {
    if (this.partitionKeyFieldName && this.sortKeyFieldName && this.tableFullName) {
      //
    }
  }

  private _fuse_getInstance() {
    return this.couchDb();
  }

  async fuse_createIndex({ indexName, fields }: { indexName: string; fields: string[] }) {
    const result = await this._fuse_getInstance()
      .getDocInstance()
      .createIndex({
        index: {
          fields: fields,
        },
        name: indexName,
        ddoc: indexName,
        type: "json",
        partitioned: true,
      });
    LoggingService.log(result);
    return {
      id: result.id,
      name: result.name,
      result: result.result,
    };
  }

  async fuse_clearAllIndexes() {
    const indexes = await this._fuse_getInstance().getIndexes();
    if (indexes?.indexes?.length) {
      for (const { ddoc, name, type } of indexes.indexes) {
        if (ddoc && name && type !== "special") {
          await this._fuse_getInstance().deleteIndex({ ddoc, name });
        }
      }
      return {
        deleted: indexes.indexes,
      };
    }
    return {
      deleted: [],
    };
  }

  fuse_getIndexes() {
    return this._fuse_getInstance().getIndexes();
  }

  async fuse_createDefinedIndexes(): Promise<string[]> {
    const results: string[] = [];
    if (this.secondaryIndexOptions?.length) {
      for (const indexOption of this.secondaryIndexOptions) {
        if (indexOption.indexName) {
          const resultData = await this.fuse_createIndex({
            fields: [indexOption.keyFieldName, indexOption.sortFieldName] as any[],
            indexName: indexOption.indexName,
          });
          LoggingService.log(resultData);
          results.push(resultData.result);
        }
      }
    }
    return results;
  }
}
