import type { IFuseCoreEntityModel } from "../core/base-schema";
import Nano from "nano";

type IBaseDef<T> = Omit<T & IFuseCoreEntityModel, "">;

interface IOptions {
  //http://admin:mypassword@localhost:5984
  couchConfig: {
    /** eg: ```127.0.0.1, localhost, example.com```  */
    host: string;
    password?: string;
    username?: string;
    databaseName: string;
    port?: number;
    /** default: ```http``` */
    protocol?: "http" | "https";
  };
  // sqliteConfig?: {
  //   dbDirectory?: string;
  //   canSplitDb?: boolean;
  // };
}

export class FuseInitializerCouch {
  private _databaseInstance!: Nano.ServerScope;
  private _documentScope!: Nano.DocumentScope<any>;

  private readonly couchConfig: IOptions["couchConfig"];
  // private readonly sqliteConfig: IOptions["sqliteConfig"];
  // readonly sqliteSplitDb: boolean;

  constructor({ couchConfig }: IOptions) {
    this.couchConfig = couchConfig;
  }

  private getFullDbUrl(config: IOptions["couchConfig"]) {
    //http://admin:mypassword@localhost:5984
    const protocol = config?.protocol || "http";
    const dbUrlPart: string[] = [`${protocol}://`];

    if (config?.username && config.password) {
      dbUrlPart.push(config.username);
      dbUrlPart.push(`:${config.password}@`);
    }

    dbUrlPart.push(config.host);

    if (config?.port) {
      dbUrlPart.push(`:${config.port}`);
    }
    return dbUrlPart.join("");
  }

  async deleteIndex({ ddoc, name }: { ddoc: string; name: string }) {
    const path = ["_index", ddoc, "json", name].join("/");
    const result: { ok: boolean } = await this._databaseInstance.request({
      db: this.couchConfig.databaseName,
      method: "DELETE",
      path,
      content_type: "application/json",
    });
    // DELETE /{db}/_index/{designdoc}/json/{name}
    return result;
  }

  async getIndexes() {
    if (this._documentScope) {
      type IIndexList = {
        indexes: {
          ddoc: string;
          name: string;
          type: string;
          def: {
            fields: {
              [field: string]: "asc" | "desc";
            }[];
          };
        }[];
        total_rows: number;
      };
      const result: IIndexList = await this._databaseInstance.request({
        db: this.couchConfig.databaseName,
        method: "GET",
        path: "_index",
        content_type: "application/json",
      });
      return result;
    }
    //GET /{db}/_index
    return null;
  }

  getDocInstance<T>(dbName?: string): Nano.DocumentScope<IBaseDef<T>> {
    if (!this._documentScope) {
      const n = this.getInstance();
      const db = n.db.use<IBaseDef<T>>(this.couchConfig.databaseName);
      this._documentScope = db;
    }
    return this._documentScope;
  }

  getInstance() {
    if (!this._databaseInstance) {
      const n = Nano(this.getFullDbUrl(this.couchConfig));
      this._databaseInstance = n;
    }
    return this._databaseInstance;
  }
}
