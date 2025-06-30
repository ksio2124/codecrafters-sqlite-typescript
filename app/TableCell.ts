import { parseSQLiteVarints32 } from "./helper";
import { Payload } from "./Payload";
import { Table } from "./Table";

const LEFT_CHILD_POINTER_BYTE_LENGTH = 4

export class TableInteriorCell {
  addr: number;
  pageBuffer: Uint8Array;
  pageView: DataView;
  constructor(addr: number, pageBuffer: Uint8Array) {
    this.addr = addr;
    this.pageBuffer = pageBuffer
    this.pageView = new DataView(pageBuffer.buffer)

  }

  getLeftChildPageNum() {
    return this.pageView.getUint32(this.addr)
  }


  getRowId() {
    // max varint is 9 bit rowid, payloadSize
    const temp = this.pageBuffer.slice(this.addr + LEFT_CHILD_POINTER_BYTE_LENGTH, this.addr + LEFT_CHILD_POINTER_BYTE_LENGTH + 9)
    return parseSQLiteVarints32(temp)[0];
  }
}

export class TableLeafCell {
  addr: number;
  pageBuffer: Uint8Array;
  pageView: DataView;
  payload: Payload
  rowId: number
  constructor(addr: number, pageBuffer: Uint8Array) {
    this.addr = addr;
    this.pageBuffer = pageBuffer
    this.pageView = new DataView(pageBuffer.buffer)
    const { value: payloadSize, bytesRead: payloadBytesRead } = this.getPayloadSize();
    const { value: rowId, bytesRead: rowIdBytesRead } = this.getRowId(addr + payloadBytesRead);
    // const rowId = this.getRowId(addr + payloadBytesRead);
    this.rowId = rowId;

    this.payload = new Payload(this.pageBuffer, addr + payloadBytesRead + rowIdBytesRead)
  }


  getPayloadSize() {
    // max varint is 9 bit rowid, payloadSize
    const temp = this.pageBuffer.slice(this.addr, this.addr + 9)
    return parseSQLiteVarints32(temp)[0];
  }

  getRowId(addr: number) {
    const temp = this.pageBuffer.slice(addr, addr + 9)
    return parseSQLiteVarints32(temp)[0];
  }
}
