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
  static LEAF_PAGE = 13;
  static INTERIOR_PAGE = 5;
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
    const schemaTypeSize = Table.getSizeFromSerialType(schemaType.value).length;
    const nameTypeSize = Table.getSizeFromSerialType(nameType.value).length;
    const namePosition = this.recordBodyPtr + schemaTypeSize;
    const nameBuffer = new DataView(this.record.buffer, namePosition, nameTypeSize);
    
    this.name = this.decoder.decode(nameBuffer);
    const tblNameSize = Table.getSizeFromSerialType(tblNameType.value).length;
    const rootPagePosition = this.recordBodyPtr + schemaTypeSize + nameTypeSize + tblNameSize;
    const rootPageType = parsed[6]
    this.rootPage = new DataView(this.record.buffer, rootPagePosition, rootPageType.value).getInt8(0);

    const sqlType = parsed[7].value
    const sqlSize = Table.getSizeFromSerialType(sqlType!).length;
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
    if (serialType === 0) return { type: 'NULL', length: 0 };
    if (serialType === 1) return { type: 'INTEGER', length: 1 };
    if (serialType === 2) return { type: 'INTEGER', length: 2 };
    if (serialType === 3) return { type: 'INTEGER', length: 3 };
    if (serialType === 4) return { type: 'INTEGER', length: 4 };
    if (serialType === 5) return { type: 'INTEGER', length: 6 };
    if (serialType === 6) return { type: 'INTEGER', length: 8 };
    if (serialType === 7) return { type: 'FLOAT', length: 8 };
    if (serialType === 8) return { type: 'INTEGER', value: 0, length: 0 };
    if (serialType === 9) return { type: 'INTEGER', value: 1, length: 0 };
    if (serialType >= 12) {
      const length = Math.floor((serialType - (serialType % 2 === 0 ? 12 : 13)) / 2);
      const type = serialType % 2 === 0 ? 'BLOB' : 'TEXT';
      return { type, length };
    }
    return { type: 'RESERVED', length: 0 };
  }

  static parseSQLiteField(dataView: DataView, offset: number, serialType: number) {
    if (serialType === 0) return { value: null, length: 0 };
    if (serialType === 1) return { value: dataView.getInt8(offset), length: 1 };
    if (serialType === 2) return { value: dataView.getInt16(offset, false), length: 2 }; // big endian
    if (serialType === 3) {
      // 3-byte int
      const val = (dataView.getUint8(offset) << 16) |
                  (dataView.getUint8(offset + 1) << 8) |
                  dataView.getUint8(offset + 2);
      // convert to signed
      const signed = val & 0x800000 ? val | 0xff000000 : val;
      return { value: signed, length: 3 };
    }
    if (serialType === 4) return { value: dataView.getInt32(offset, false), length: 4 };
    if (serialType === 5) {
      // 6-byte int
      const high = dataView.getUint16(offset, false);
      const low = dataView.getUint32(offset + 2, false);
      return { value: BigInt(high) << 32n | BigInt(low), length: 6 };
    }
    if (serialType === 6) return { value: dataView.getBigInt64(offset, false), length: 8 };
    if (serialType === 7) return { value: dataView.getFloat64(offset, false), length: 8 };
    if (serialType === 8) return { value: 0, length: 0 };
    if (serialType === 9) return { value: 1, length: 0 };
    if (serialType >= 12) {
      const isText = serialType % 2 === 1;
      const byteLen = Math.floor((serialType - (isText ? 13 : 12)) / 2);
      const bytes = new Uint8Array(dataView.buffer, dataView.byteOffset + offset, byteLen);
      const value = isText
        ? new TextDecoder().decode(bytes)
        : bytes;
      return { value, length: byteLen };
    }
    return { value: undefined, length: 0 };
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

  async getAllPages(pageBuffer: Uint8Array) {
    let pages: [number, number][] = [];
    let leafPages: [number, number][] = [];
    const view = new DataView(pageBuffer.buffer, 0, pageBuffer.byteLength);
    const pageType = view.getUint8(0);
    if (pageType === Table.INTERIOR_PAGE) {
      const cellCount = view.getUint16(3);
      const rightMostPointer = view.getUint32(8);
      const cellPtrs = [];
      for (let i = 0; i < cellCount; i++) {
        cellPtrs.push(view.getUint16(12 + i * 2));
      }
      for (let i = 0; i < cellPtrs.length; i++) {
        const pageNum = view.getUint32(cellPtrs[i])
        const rowIdStart = cellPtrs[i] + 4;
        const rowIdEnd = rowIdStart + 9
        const rowIdBuffer = pageBuffer.slice(rowIdStart, rowIdEnd);
        const rowId =  parseSQLiteVarints32(rowIdBuffer)[0].value;
        pages.push([pageNum, rowId]);
      }
      for (let i = 0; i < pages.length; i++) {
        const pageBuffer = await this.getPageBuffer(pages[i][0]);
        const res = await this.getAllPages(pageBuffer);
        if (res === true) {
          leafPages.push(pages[i])
        } else {
          leafPages = [...leafPages, ...res]
        }
      }
      return leafPages
    }
    return true;
  }

  
  async getPageBuffer(pageNum: number) {
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
    const offset = (pageNum - 1) * this.database.pageSize;
    await databaseFileHandler.read(buffer, 0, buffer.length, offset);
    return buffer;
  }

  async getAllRowsFromPage(pageNum: number) {
    const pageBuffer = await this.getPageBuffer(pageNum);
    const view = new DataView(pageBuffer.buffer, 0, pageBuffer.byteLength);
    const numberOfRows = view.getUint16(Table.LEAF_PAGE_NUMBER_OF_CELL.position);
    const cellPtrs = [];
    for (let i = 0; i < numberOfRows; i++) {
      const cellOffset = view.getInt16(8 + i * 2);
      cellPtrs.push(cellOffset);
    }
    for (let i = 0; i < cellPtrs.length; i++) {
      const [cellRecordSize, rowId] = parseSQLiteVarints32(pageBuffer.slice(cellPtrs[i], cellPtrs[i] + 18));
      const cellHeaderOffset = cellRecordSize.bytesRead + rowId.bytesRead;
      const row = new Row(this, rowId.value, pageBuffer.slice(cellPtrs[i] + cellHeaderOffset )) 
      row.init();
      this.rows.push(row)
    }
    return this.rows;
  }


  async getAllRows() {
    const pageBuffer = await this.getRootPage()
    const allPages = await this.getAllPages(pageBuffer);
    if (allPages === true) {
      await this.getAllRowsFromPage(this.rootPage!)
    } else {
      for (let i = 0; i < allPages.length; i++) {
        // console.log('pageNum', allPages[i][0])
        await this.getAllRowsFromPage(allPages[i][0])
      }
    }
    // console.log(this.rows)
    return this.rows;
  }

  async getRootPage() {
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
    const pageBuffer = await this.getRootPage();
    return new DataView(pageBuffer.buffer, 0, pageBuffer.byteLength).getUint16(Table.LEAF_PAGE_NUMBER_OF_CELL.position);
  }
}
