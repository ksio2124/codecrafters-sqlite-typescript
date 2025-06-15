import { open } from "fs/promises";
import { constants } from "fs";
import type { FileHandle } from "fs/promises";

import {Table} from './Table'
export class Database {
  databaseFileHandler?: FileHandle;
  pageSize?: number;
  tableSize?: number;
  tables: Table[];
  databaseFilePath: string;
  dataView?: DataView;
  constructor(databaseFilePath: string) {
    this.databaseFilePath = databaseFilePath;
    this.tables = [];
  }
  async init(): Promise<void> {
    this.databaseFileHandler = await open(
      this.databaseFilePath,
      constants.O_RDONLY
    );
    let buffer: Uint8Array = new Uint8Array(105);
    await this.databaseFileHandler.read(buffer, 0, buffer.length, 0);
    this.pageSize = new DataView(buffer.buffer, 0, buffer.byteLength).getUint16(
      16
    );
    buffer = new Uint8Array(this.pageSize);
    await this.databaseFileHandler.read(buffer, 0, buffer.length, 0);
    this.dataView = new DataView(buffer.buffer, 0, buffer.byteLength);

    this.tableSize = this.dataView.getUint16(103)
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
    const buffer = new Uint8Array(this.pageSize);
    await this.databaseFileHandler.read(buffer, 0, buffer.length, 0);
    const view = new DataView(buffer.buffer, 0, buffer.byteLength);
    for (let i = 0; i < this.tableSize; i++) {
      const recordPtr = view.getUint16(108 + i * 2);
      const table = new Table(recordPtr, this);
      await table.init();
      this.tables.push(table);
    }
  }
}