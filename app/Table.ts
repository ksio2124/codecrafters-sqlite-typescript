import {Database} from './Database'
import type { FileHandle } from 'fs/promises';
import { Row } from './Row';
import { decodeVarint, parseSQLiteVarints32 } from './helper';

export class Table {
  name?: string;
  recordPtr: number;
  database: Database;
  rootPage?: number;
  sql?: string;
  record?: Uint8Array;
  recordBodyPtr?: number;
  type?:string;
  tblName?:string;
  decoder:TextDecoder;
  rows: Row[];
  recordHeaderPtr?: number;
  static LEAF_PAGE_NUMBER_OF_CELL = {position: 3, size: 2};
  static LEAF_PAGE_CELL_CONTENT_AREA = {position: 5, size: 2};
  static LEAF_PAGE_CELL_ROW_ID = {position: 2, size: 1};
  constructor(recordPtr: number, database: Database) {
    this.recordPtr = recordPtr;
    this.database = database;
    this.decoder = new TextDecoder();
    this.rows = [];
  }

  async init(): Promise<void> {
    if (!this.database.checkInit()) {
      throw new Error("Database not initialized. Call init() first.");
    }
    [this.record, this.recordHeaderPtr] = await this.getRecord(
      this.database.databaseFileHandler
    );
    const parsed = parseSQLiteVarints32(this.record)
    const [_,skip, recordHeaderSize, schemaType, nameType, tblNameType]= parsed;
    this.recordBodyPtr = this.recordHeaderPtr + recordHeaderSize.value;
    const schemaTypeSize = Table.getSizeFromSerialType(schemaType.value);
    const nameTypeSize = Table.getSizeFromSerialType(nameType.value);
    const namePosition = this.recordBodyPtr + schemaTypeSize;
    const nameBuffer = new DataView(this.record.buffer, namePosition, nameTypeSize);
    
    this.name = this.decoder.decode(nameBuffer);
    const tblNameSize = Table.getSizeFromSerialType(tblNameType.value);
    const rootPagePosition = this.recordBodyPtr + schemaTypeSize + nameTypeSize + tblNameSize;
    const rootPageType = parsed[6]
    this.rootPage = new DataView(this.record.buffer, rootPagePosition, rootPageType.value).getInt8(0);

    const sqlType = parsed[7].value
    const sqlSize = Table.getSizeFromSerialType(sqlType!);
    const sqlPosition = this.recordBodyPtr + schemaTypeSize + nameTypeSize + tblNameSize + rootPageType.value;
    const sqlBuffer = new DataView(this.record.buffer, sqlPosition, sqlSize);
    this.sql = this.decoder.decode(sqlBuffer);
  }

  checkInit(): this is Table & {
    name: string;
    rootPage: number;
    sql: string;
  } {
    if (
      this.name === undefined ||
      this.rootPage === undefined ||
      this.sql === undefined
    ) {
      return false;
    }
    return true;
  }

  static getSizeFromSerialType(serialType: number) {
    return (serialType - 13) / 2;
  }

  async getRecord(databaseFileHandler: FileHandle): Promise<[Uint8Array, number]> {
    let buffer: Uint8Array = new Uint8Array(9);
    await databaseFileHandler.read(buffer, 0, buffer.length, this.recordPtr);
    const parsed = parseSQLiteVarints32(buffer)[0];
    let recordSize = parsed.value;
    // varint of recordSize
    recordSize += parsed.bytesRead;
    // adding 1 more for beginning of Record
    recordSize++;
    buffer = new Uint8Array(recordSize);
    await databaseFileHandler.read(buffer, 0, buffer.length, this.recordPtr);
    const recordHeaderPtr = parsed.bytesRead + 1 // rowId
    return [buffer, recordHeaderPtr];
  }

  getColumnNames(sql: string) {
    const openParenIdx = sql.indexOf('(');
    const closeParenIdx = sql.lastIndexOf(')');
    const columnString = sql.slice(openParenIdx + 1, closeParenIdx);
    return columnString.split(',').map((column) => column.trim().split(' ')[0]);
  }

  getRowsWithColumnNames() {
    if (!this.checkInit()) {
      throw new Error('Table not initialized');
    }
    console.log(this.getColumnNames(this.sql))
  }

  async getAllRows() {
    const pageBuffer = await this.getPage()
    const numberOfRows = new DataView(pageBuffer.buffer, 0, pageBuffer.byteLength).getUint16(Table.LEAF_PAGE_NUMBER_OF_CELL.position);
    const cellContentAreaPtr = new DataView(pageBuffer.buffer, 0, pageBuffer.byteLength).getUint16(Table.LEAF_PAGE_CELL_CONTENT_AREA.position);
    let offset = 0;
    let count = 0;
    while (count < numberOfRows) {
      let cellRecordSize = new DataView(pageBuffer.buffer, cellContentAreaPtr + offset, 1).getUint8(0);
      let rowId = new DataView(pageBuffer.buffer, cellContentAreaPtr + offset + Table.LEAF_PAGE_CELL_ROW_ID.position, Table.LEAF_PAGE_CELL_ROW_ID.size).getUint8(0);      
      let row = new Row(this, rowId, pageBuffer.slice(cellContentAreaPtr + offset, cellContentAreaPtr + offset + cellRecordSize + 2));
      offset += row.init();
      count++;
      this.rows.push(row)
    }

    return this.rows;
  }

  async getPage() {
    if (!this.database.checkInit()) {
      throw new Error("Database not initialized. Call init() first.");
    }
    if (!this.checkInit()) {
      throw new Error(
        "Table does not have a root page. Call init() first."
      );
    }
    const databaseFileHandler = this.database.databaseFileHandler;
    const buffer = new Uint8Array(this.database.pageSize);
    const offset = (this.rootPage - 1) * this.database.pageSize;
    await databaseFileHandler.read(buffer, 0, buffer.length, offset);
    return buffer;
  }

  async getNumberOfRows(): Promise<number> {
    const pageBuffer = await this.getPage();
    return new DataView(pageBuffer.buffer, 0, pageBuffer.byteLength).getUint16(Table.LEAF_PAGE_NUMBER_OF_CELL.position);
  }
}
