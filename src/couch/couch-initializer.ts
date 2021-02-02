import PouchDB from "pouchdb";
import os from "os";
import path from "path";
import fs from "fs";
import { IDynamoDataCoreEntityModel } from "../core/base-schema";

PouchDB.plugin(require("pouchdb-find"));
PouchDB.plugin(require("pouchdb-adapter-node-websql"));
PouchDB.plugin(require("pouchdb-adapter-http"));
// PouchDB.plugin(require("pouchdb-debug"));

type IBaseDef<T> = Omit<T, "">;

interface IOptions {
  couchConfig?: {
    dbUrl: string;
    password?: string;
    username?: string;
  };
  sqliteConfig?: {
    dbDirectory: string;
  };
}

export class FuseInitializerCouch {
  private _databaseInstance!: PouchDB.Database<any>;

  private readonly couchConfig: IOptions["couchConfig"];
  private readonly sqliteConfig: IOptions["sqliteConfig"];

  constructor({ couchConfig, sqliteConfig }: IOptions) {
    this.couchConfig = couchConfig;
    this.sqliteConfig = sqliteConfig;
  }

  private createWebSqlInstance({ dbPath }: { dbPath: string }) {
    return new PouchDB<IBaseDef<IDynamoDataCoreEntityModel>>(dbPath, {
      adapter: "websql",
    });
  }

  private createHttpInstance({ dbUrl, password, username }: { dbUrl: string; password?: string; username?: string }) {
    if (username && password) {
      return new PouchDB<IBaseDef<IDynamoDataCoreEntityModel>>(dbUrl, {
        auth: {
          password,
          username,
        },
      });
    }
    return new PouchDB<IBaseDef<IDynamoDataCoreEntityModel>>(dbUrl);
  }

  getInstance<T>(sqliteDbName?: string): PouchDB.Database<IBaseDef<T>> {
    if (!this._databaseInstance) {
      if (this.couchConfig?.dbUrl) {
        this._databaseInstance = this.createHttpInstance({
          dbUrl: this.couchConfig.dbUrl /* "http://localhost:5984/my-database" */,
          password: this.couchConfig.password,
          username: this.couchConfig.username,
        });
      } else {
        const baseSqliteDir = this.sqliteConfig?.dbDirectory || path.resolve(`${os.homedir()}/pouch_db_out`);

        if (!fs.existsSync(baseSqliteDir)) {
          fs.mkdirSync(baseSqliteDir, { recursive: true });
        }

        if (sqliteDbName) {
          this._databaseInstance = this.createWebSqlInstance({
            dbPath: path.resolve(`${baseSqliteDir}/${sqliteDbName}.db`),
          });
        } else {
          this._databaseInstance = this.createWebSqlInstance({
            dbPath: path.resolve(`${baseSqliteDir}/pouch_db_5bc365dc993c682b921d21744cf9b72b.db`),
          });
        }
      }

      // if (envConfig.NODE_ENV === "development") {
      //   PouchDB.debug.enable("*");
      //   this._databaseInstance.info().then((info) => {
      //     console.log(info);
      //   });
      // } else {
      //   PouchDB.debug.disable();
      // }

      // if (envConfig.APP_REPLICATION_DATABASE_URL && envConfig.REPLICATION_ENABLED) {
      //   const remoteDB = this.createHttpInstance({
      //     dbUrl: envConfig.APP_REPLICATION_DATABASE_URL,
      //     password: "ss@yeur",
      //     username: "me-user",
      //   });
      //   this._databaseInstance.replicate
      //     .to(remoteDB)
      //     .on("complete", () => {
      //       console.log("Romote DB Replication Successful");
      //     })
      //     .on("error", (err) => {
      //       console.log("Romote DB Replication Error", err);
      //     });
      // }
    }
    return this._databaseInstance;
  }

  compactDb() {
    this._databaseInstance
      .compact({ interval: undefined })
      .then((info) => {
        // compaction complete
        console.log("compaction complete");
      })
      .catch((err) => {
        // handle errors
        console.log("compaction erros", err);
      });
  }
}
