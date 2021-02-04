import { LoggingService } from "./../helpers/logging-service";
import { IFuseIndexDefinition } from "./../type/types";

interface ITableOptions<T> {
  couchDb: () => PouchDB.Database<Pick<unknown, never>>;
  secondaryIndexOptions: IFuseIndexDefinition<T>[];
  tableFullName: string;
  partitionKeyFieldName: string;
  sortKeyFieldName: string;
}

export class CouchManageTable<T> {
  private readonly partitionKeyFieldName: string;
  private readonly sortKeyFieldName: string;
  private readonly couchDb: () => PouchDB.Database<Pick<unknown, never>>;
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

  async fuse_createIndex({ indexName, fields }: { indexName: string; fields: string[] }): Promise<string> {
    const result = await this._fuse_getInstance().createIndex({
      index: {
        name: indexName,
        fields: fields,
        ddoc: indexName,
        type: "json",
      },
    });
    LoggingService.log(result);
    return result?.result;
  }

  async fuse_clearAllIndexes() {
    const indexes = await this._fuse_getInstance().getIndexes();
    for (const { ddoc, name, type } of indexes?.indexes) {
      if (ddoc && name && type !== "special") {
        await this._fuse_getInstance().deleteIndex({ ddoc, name });
      }
    }
  }

  fuse_getIndexes() {
    return this._fuse_getInstance().getIndexes();
  }

  async fuse_createDefinedIndexes(): Promise<string[]> {
    const results: string[] = [];
    if (this.secondaryIndexOptions?.length) {
      for (const indexOption of this.secondaryIndexOptions) {
        if (indexOption.indexName) {
          const resultData = await this._fuse_getInstance().createIndex({
            index: {
              name: indexOption.indexName,
              fields: [indexOption.keyFieldName, indexOption.sortFieldName] as any[],
              ddoc: indexOption.indexName,
              type: "json",
            },
          });
          LoggingService.log(resultData);
          results.push(resultData.result);
        }
      }
    }
    return results;
  }
}
