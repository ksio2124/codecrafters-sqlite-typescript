import { open } from "fs/promises";
import { constants } from "fs";
import type { FileHandle } from "fs/promises";

const args = process.argv;
const databaseFilePath: string = args[2];
const command: string = args[3];

class Database {
  databaseFileHandler?: FileHandle;
  pageSize?: number;
  tableSize?: number;
  tables: Table[];
  databaseFilePath: string;
  constructor(databaseFilePath: string) {
    this.databaseFilePath = databaseFilePath;
    this.tables = [];
  }
  async init(): Promise<void> {
    this.databaseFileHandler = await open(
      this.databaseFilePath,
      constants.O_RDONLY
    );
    const buffer: Uint8Array = new Uint8Array(105);
    await this.databaseFileHandler.read(buffer, 0, buffer.length, 0);
    this.pageSize = new DataView(buffer.buffer, 0, buffer.byteLength).getUint16(
      16
    );
    this.tableSize = new DataView(
      buffer.buffer,
      0,
      buffer.byteLength
    ).getUint16(103);
  }
  async close(): Promise<void> {
    if (this.databaseFileHandler) {
      await this.databaseFileHandler.close();
      this.databaseFileHandler = undefined;
    }
    this.pageSize = undefined;
    this.tableSize = undefined;
    this.tables = [];
  }
  checkInit(): this is Database & {
    databaseFileHandler: FileHandle;
    pageSize: number;
    tableSize: number;
  } {
    if (
      this.databaseFileHandler === undefined ||
      this.pageSize === undefined ||
      this.tableSize === undefined
    ) {
      return false;
    }
    return true;
  }

  async findAllTables(): Promise<void> {
    if (!this.checkInit()) {
      throw new Error("Database not initialized. Call init() first.");
    }
    const buffer = new Uint8Array(2 * this.tableSize);
    await this.databaseFileHandler.read(buffer, 0, buffer.length, 108);
    for (let i = 0; i < this.tableSize; i++) {
      const recordPtr = new DataView(buffer.buffer, i * 2, 2).getUint16(0);
      const table = new Table(recordPtr, this);
      await table.getTableName();
      this.tables.push(table);
    }
  }
}

class Table {
  name?: string;
  recordPtr: number;
  Database: Database;
  rootPage?: number;
  constructor(recordPtr: number, database: Database) {
    this.recordPtr = recordPtr;
    this.Database = database;
  }
  async getTableName(): Promise<void> {
    if (!this.Database.checkInit()) {
      throw new Error("Database not initialized. Call init() first.");
    }
    const databaseFileHandler = this.Database.databaseFileHandler;
    let buffer: Uint8Array = new Uint8Array(4);
    await databaseFileHandler.read(buffer, 0, buffer.length, this.recordPtr);
    const recordSize = new DataView(
      buffer.buffer,
      0,
      buffer.byteLength
    ).getUint8(0);
    buffer = new Uint8Array(recordSize);
    await databaseFileHandler.read(buffer, 0, buffer.length, this.recordPtr);
    const recordHeaderSize = new DataView(
      buffer.buffer,
      0,
      buffer.byteLength
    ).getUint8(2);
    const type = new DataView(buffer.buffer, 0, buffer.byteLength).getUint8(3);
    const name = new DataView(buffer.buffer, 0, buffer.byteLength).getUint8(4);
    const tblName = new DataView(buffer.buffer, 0, buffer.byteLength).getUint8(
      5
    );
    const recordBodyPtr = recordHeaderSize + 2;
    const tableNameOffset = (type - 13) / 2 + (name - 13) / 2;
    const tableNamePtr = recordBodyPtr + tableNameOffset;
    const tableNameSize = (tblName - 13) / 2;
    const tableNameBuffer = new DataView(
      buffer.buffer,
      tableNamePtr,
      tableNameSize
    );
    this.rootPage = new DataView(
      buffer.buffer,
      tableNamePtr + tableNameSize,
      4
    ).getUint8(0);
    const decoder = new TextDecoder();
    this.name = decoder.decode(tableNameBuffer);
  }

  checkHasRootPage(): this is Table & { rootPage: number } {
    if (this.rootPage === undefined) {
      return false;
    }
    return true;
  }

  async getNumberOfRows(): Promise<number> {
    if (!this.Database.checkInit()) {
      throw new Error("Database not initialized. Call init() first.");
    }
    if (!this.checkHasRootPage()) {
      throw new Error(
        "Table does not have a root page. Call getTableName() first."
      );
    }
    const databaseFileHandler = this.Database.databaseFileHandler;
    const buffer = new Uint8Array(2);
    // console.log(this.name, (this.rootPage - 1) * this.Database.pageSize);
    const offset = (this.rootPage - 1) * this.Database.pageSize + 3;
    await databaseFileHandler.read(
      buffer,
      0,
      buffer.length,
      offset
    );
    return new DataView(buffer.buffer, 0, buffer.byteLength).getUint16(0);
  }
}

const database = new Database(databaseFilePath);
await database.init();
await database.findAllTables();

if (command === ".dbinfo") {
  console.log(`database page size: ${database.pageSize}`);
  console.log(`number of tables: ${database.tableSize}`);
} else if (command === ".tables") {
  console.log(database.tables.map((table) => table.name).join(" "));
} else if (command.startsWith("SELECT COUNT(*) FROM")) {
  const tableName = command.split(" ").at(-1);
  const table = database.tables.filter((table) => table.name === tableName)[0];
  console.log(await table.getNumberOfRows());
} else {
  throw new Error(`Unknown command ${command}`);
}

await database.close();
