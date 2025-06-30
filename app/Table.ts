import { Database } from './Database'
import { Row } from './Row';
import { parseSQLiteVarints32 } from './helper';
import { INDEX_INTERIOR_PAGE, INDEX_LEAF_PAGE, NUMBER_OF_CELL, PAGE_TYPE, TABLE_INTERIOR_PAGE, TABLE_LEAF_PAGE } from './const';
import { IndexInteriorPage, IndexLeafPage } from './IndexPage';
import { RecordBody } from './Payload';
import { TableInteriorPage, TableLeafPage } from './TablePage';
import { TableLeafCell } from './TableCell';

export class Table {
  name?: string;
  recordPtr: number;
  database: Database;
  rootPage?: number;
  sql?: string;
  decoder: TextDecoder;
  rows: Row[];
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

    let pageBuffer: Uint8Array = new Uint8Array(this.database.pageSize);
    const databaseFileHandler = this.database.databaseFileHandler
    await databaseFileHandler.read(pageBuffer, 0, pageBuffer.length, 0);

    const cell = new TableLeafCell(this.recordPtr, pageBuffer);
    const [_, name, tbl_name, rootPage, sql] = cell.payload.recordBody.keys;
    this.name = name.value;
    this.rootPage = rootPage.value;
    this.sql = sql.value;
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
    if (pageType === TABLE_INTERIOR_PAGE) {
      const cellCount = view.getUint16(3);
      const cellPtrs = [];
      for (let i = 0; i < cellCount; i++) {
        cellPtrs.push(view.getUint16(12 + i * 2));
      }
      for (let i = 0; i < cellPtrs.length; i++) {
        const pageNum = view.getUint32(cellPtrs[i])
        const rowIdStart = cellPtrs[i] + 4;
        const rowIdEnd = rowIdStart + 9
        const rowIdBuffer = pageBuffer.slice(rowIdStart, rowIdEnd);
        const rowId = parseSQLiteVarints32(rowIdBuffer)[0].value;
        pages.push([pageNum, rowId]);
      }
      for (let i = 0; i < pages.length; i++) {
        const pageBuffer = await this.database.getPageBuffer(pages[i][0]);
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


  async getAllRowsFromPage(pageNum: number) {
    const pageBuffer = await this.database.getPageBuffer(pageNum);
    const view = new DataView(pageBuffer.buffer, 0, pageBuffer.byteLength);
    const numberOfRows = view.getUint16(NUMBER_OF_CELL.offset);
    const cellPtrs = [];
    for (let i = 0; i < numberOfRows; i++) {
      const cellOffset = view.getInt16(8 + i * 2);
      cellPtrs.push(cellOffset);
    }
    for (let i = 0; i < cellPtrs.length; i++) {
      const [cellRecordSize, rowId] = parseSQLiteVarints32(pageBuffer.slice(cellPtrs[i], cellPtrs[i] + 18));
      const cellHeaderOffset = cellRecordSize.bytesRead + rowId.bytesRead;
      const row = new Row(this, rowId.value, pageBuffer.slice(cellPtrs[i] + cellHeaderOffset))
      row.init();
      this.rows.push(row)
    }
    return this.rows;
  }

  async getRecordOnIdsHelper(ids: number[], idsIdx: number, res: RecordBody[], pageNum: number, leftVal = 0) {
    const pageBuffer = await this.database.getPageBuffer(pageNum);
    const pageView = new DataView(pageBuffer.buffer);
    const pageType = pageView.getUint8(PAGE_TYPE.offset)
    let idsIdx2 = idsIdx;
    if (pageType === TABLE_INTERIOR_PAGE) {
      const page = new TableInteriorPage(pageBuffer);
      const cells = page.getCells();
      let lowestVal = leftVal;
      let rowId = cells[0].getRowId().value;
      for (let i = 0; i < cells.length; i++) {
        const cell = cells[i];
        rowId = cell.getRowId().value;
        if (ids[idsIdx2] <= rowId && ids[idsIdx2] >= lowestVal) {
          const { idsIdx2: newIdsIdx2 } = await this.getRecordOnIdsHelper(ids, idsIdx2, res, cell.getLeftChildPageNum(), rowId);
          idsIdx2 = newIdsIdx2;
        }
        lowestVal = rowId;
      }
      if (ids[idsIdx2] >= rowId) {
        const nextPage = page.getRightMostPageNum();
        const { idsIdx2: newIdsIdx2 } = await this.getRecordOnIdsHelper(ids, idsIdx2, res, nextPage, rowId);
        idsIdx2 = newIdsIdx2;
      }
    } else if (pageType === TABLE_LEAF_PAGE) {
      const page = new TableLeafPage(pageBuffer);
      const cells = page.getCells();
      for (let i = 0; i < cells.length; i++) {
        const cell = cells[i];
        const rowId = cell.rowId;
        if (rowId === ids[idsIdx2]) {
          res.push(cell.payload.recordBody);
          idsIdx2++;
        }
      }
    }

    return { res, idsIdx2 };
  }



  // assume id is sorted from smallest to greatest
  async getRecordOnIds(ids: number[]) {
    return await this.getRecordOnIdsHelper(ids, 0, [], this.rootPage!);
  }



  async getMatchingIndexHelper(value: string, leftVal: string, res: number[], pageNum: number) {
    const pageBuffer = await this.database.getPageBuffer(pageNum);
    const pageView = new DataView(pageBuffer.buffer);
    const pageType = pageView.getUint8(PAGE_TYPE.offset)
    if (pageType === INDEX_INTERIOR_PAGE) {
      const page = new IndexInteriorPage(pageBuffer);
      const cells = page.getCells();
      let lowestVal = leftVal;
      let [curVal, rowId] = cells[0].payload.recordBody.keys;
      for (let i = 0; i < cells.length; i++) {
        const cell = cells[i];
        [curVal, rowId] = cell.payload.recordBody.keys;
        if (value === curVal.value) {
          res.push(rowId.value)
        }
        if (value <= curVal.value && value >= lowestVal) {
          const nextPage = cell.getLeftChildPageNum();
          await this.getMatchingIndexHelper(value, curVal.value, res, nextPage);
        }
        lowestVal = curVal.value;
      }
      if (value >= curVal.value) {
        const nextPage = page.getRightMostPageNum();
        await this.getMatchingIndexHelper(value, curVal.value, res, nextPage)
      }
    } else if (pageType === INDEX_LEAF_PAGE) {
      const page = new IndexLeafPage(pageBuffer);
      const cells = page.getCells();
      for (let i = 0; i < cells.length; i++) {
        const cell = cells[i];
        const [curVal, rowId] = cell.payload.recordBody.keys;
        if (value === curVal.value) {
          res.push(rowId.value)
        }
      }
    }
    return res;
  }

  async getMatchingIndex(value: string) {
    return await this.getMatchingIndexHelper(value, '', [], this.rootPage!)

  }

  async getAllRows() {
    const pageBuffer = await this.database.getPageBuffer(this.rootPage!)
    const allPages = await this.getAllPages(pageBuffer);
    if (allPages === true) {
      await this.getAllRowsFromPage(this.rootPage!)
    } else {
      for (let i = 0; i < allPages.length; i++) {
        await this.getAllRowsFromPage(allPages[i][0])
      }
    }
    return this.rows;
  }
}
