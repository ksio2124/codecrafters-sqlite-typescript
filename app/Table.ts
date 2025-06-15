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
  // from the beginning of the RECORD on RECORD_HEADER
  static RECORD_SIZE = {position: 4, size: 1};
  static RECORD_HEADER_POSITION = 2;
  static RECORD_SCHEMA_TYPE = {position: 3, size: 1};
  static RECORD_SCHEMA_NAME = {position: 4, size: 1};
  static RECORD_SCHEMA_TBL_NAME = {position: 5, size: 1};
  static RECORD_SCHEMA_ROOT_PAGE = {position: 6, size: 1};
  static RECORD_SCHEMA_SQL = {position: 7, size: 2};
  static ROW_ID_LENGTH = 1;
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

        // const recordHeaderSizeBuffer = this.cellBuffer.slice(2, 4);
        // const recordHeaderSize = decodeVarint(recordHeaderSizeBuffer);
        // const endOfRecordHeader = 2 + recordHeaderSize!;
        // const recordHeaderBuffer = this.cellBuffer.slice(2, endOfRecordHeader);
        // let res = parseSQLiteVarints32(recordHeaderBuffer);
        // res = res.slice(2, res.length);
    // [recordSize, type, name, tbl_name, rootPage, sql]
    // this.recordBodyPtr = this.getRecordBodyPtr(this.record);

    // this need to be fixed
    // const recordHeaderBuffer = this.record.slice(2,this.recordBodyPtr)

    // find recordHeaderSize
    // let buffer = this.record.slice(this.recordHeaderPtr, this.recordHeaderPtr + 9);
    const parsed = parseSQLiteVarints32(this.record)

    // let buffer = this.record.slice(this.recordHeaderPtr, this.recordHeaderPtr + 9);
    // const parsed = parseSQLiteVarints32(buffer);
    // // console.log('parsed',parsed)
    const [_,skip, recordHeaderSize, schemaType, nameType, tblNameType]= parsed;
    // console.log('recordHeaderSize', recordHeaderSize.value)
    this.recordBodyPtr = this.recordHeaderPtr + recordHeaderSize.value;

    // then recordHeaderPtr + recordHeaderSize = recordBodyPtr

    // const schemaType = this.getSchemaType(this.record);
    const schemaTypeSize = Table.getSizeFromSerialType(schemaType.value);
    // const nameType = this.getNameType(this.record);
    const nameTypeSize = Table.getSizeFromSerialType(nameType.value);
    const namePosition = this.recordBodyPtr + schemaTypeSize;
    // console.log('name position', namePosition);
    const nameBuffer = new DataView(this.record.buffer, namePosition, nameTypeSize);
    
    this.name = this.decoder.decode(nameBuffer);
    // const tblNameType = this.getTblNameType(this.record);
    const tblNameSize = Table.getSizeFromSerialType(tblNameType.value);
    const rootPagePosition = this.recordBodyPtr + schemaTypeSize + nameTypeSize + tblNameSize;
    // rootPageType is twos complement
    const rootPageType = parsed[6]
    this.rootPage = new DataView(this.record.buffer, rootPagePosition, rootPageType.value).getInt8(0);

    const sqlType = parsed[7].value
    const sqlSize = Table.getSizeFromSerialType(sqlType!);
    // console.log(sqlType, sqlSize, this.getSqlType(this.record))
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
    // let recordSize = parseSQLiteVarints32(buffer)[0].value;
    const parsed = parseSQLiteVarints32(buffer)[0];
    let recordSize = parsed.value;
    // varint of recordSize
    recordSize += parsed.bytesRead;
    // adding 1 more for beginning of Record
    recordSize++;
    buffer = new Uint8Array(recordSize);
    // console.log('record Size ', recordSize, 'bytesRead', parsed.bytesRead)
    await databaseFileHandler.read(buffer, 0, buffer.length, this.recordPtr);
    const recordHeaderPtr = parsed.bytesRead + 1 // rowId
    // console.log(recordHeaderPtr);
    return [buffer, recordHeaderPtr];
  }

  getRecordBodyPtr(record: Uint8Array): number {
    const recordHeaderSize = new DataView(
      record.buffer,
      0,
      record.byteLength
      // first byte of RECORD_HEADER is size of RECORD_HEADER
    ).getUint8(Table.RECORD_HEADER_POSITION);

    return Table.RECORD_HEADER_POSITION + recordHeaderSize;
  }

  getSchemaType(record: Uint8Array): number {
    return new DataView(record.buffer, 0, record.byteLength).getUint8(Table.RECORD_SCHEMA_TYPE.position);
  }

  getNameType(record:Uint8Array): number {
    return new DataView(record.buffer, 0, record.byteLength).getUint8(Table.RECORD_SCHEMA_NAME.position);
  }

  getTblNameType(record:Uint8Array): number {
    return new DataView(record.buffer, 0, record.byteLength).getUint8(Table.RECORD_SCHEMA_TBL_NAME.position);
  }

  getRootPage(record:Uint8Array): number {
    // console.(record.buffer);
    return new DataView(record.buffer, 0, record.byteLength).getInt8(Table.RECORD_SCHEMA_ROOT_PAGE.position);
  }

  getSqlType(record:Uint8Array): Uint8Array {
    const sqlEnd = Table.RECORD_SCHEMA_SQL.position + Table.RECORD_SCHEMA_SQL.size;
    return record.slice(Table.RECORD_SCHEMA_SQL.position, sqlEnd);
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

    // this.rows.map(row => console.log(row.content))
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
