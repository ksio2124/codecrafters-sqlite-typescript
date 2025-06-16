import { Table } from "./Table";
import { parseSQLiteVarints32, decodeVarint } from "./helper";
export class Row {
  content: Record<string, any>;
  table: Table;
  rowId: number;
  cellBuffer: Uint8Array;
  static ROW_SIZE_POSITION = 2;
  columnTypes: number[];
  recordBodyPtr?: number;
  constructor(table: Table, rowId: number, cellBuffer: Uint8Array) {
    this.content = {};
    this.table = table;
    this.rowId = rowId;
    this.cellBuffer = cellBuffer;
    this.columnTypes = [];
    // this.init();
  }
  // get each columntype
  init() {
    const recordHeaderSizeBuffer = this.cellBuffer.slice(0, 9);
    const recordHeaderSize = decodeVarint(recordHeaderSizeBuffer);
    const endOfRecordHeader = recordHeaderSize!;
    const recordHeaderBuffer = this.cellBuffer.slice(2, endOfRecordHeader);
    let res = parseSQLiteVarints32(recordHeaderBuffer);
    // res = res.slice(2, res.length);
    let cursor = endOfRecordHeader;
    let count = 0;
    const columnNames = this.table.getColumnNames(this.table.sql!).slice(1);
    // console.log(columnNames);/
      while (count < res.length) {
        const size = Table.getSizeFromSerialType(res[count].value).length;
        const value = this.table.decoder.decode(
          new DataView(this.cellBuffer.buffer, cursor, size)
        );
        const columnName = columnNames[count];
        this.content[columnName] = value;
        cursor += size;
        count++;
      }
    // console.log(this.content)
    return cursor;
  }


}
