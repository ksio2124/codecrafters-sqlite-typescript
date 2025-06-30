import { parseSQLiteVarints32 } from "./helper"
import { Table } from "./Table"

export class Payload {
  pageBuffer: Uint8Array
  addr: number
  recordHeader: RecordHeader
  recordBody: RecordBody
  constructor(pageBuffer: Uint8Array, addr: number) {
    this.pageBuffer = pageBuffer
    this.addr = addr
    this.recordHeader = new RecordHeader(this.pageBuffer, this.addr);
    this.recordBody = new RecordBody(this.recordHeader, this.pageBuffer)
  }
}

class RecordHeader {
  headerSize: number
  serialTypes: number[]
  addr: number
  constructor(pageBuffer: Uint8Array, addr: number) {
    this.addr = addr
    const headerSizeBuffer = pageBuffer.slice(addr, addr + 9);

    const { value: headerSize, offset: headerSizeOffset } = parseSQLiteVarints32(headerSizeBuffer)[0];
    this.headerSize = headerSize;
    const recordHeader = parseSQLiteVarints32(pageBuffer.slice(addr, addr + headerSize));
    // console.log('recordHeader', recordHeader, headerSize, addr)
    this.serialTypes = recordHeader.slice(1).map((val) => val.value);
  }
}

export class RecordBody {
  recordHeader: RecordHeader
  pageBuffer: Uint8Array
  keys: any[]
  // rowId: number
  constructor(recordHeader: RecordHeader, pageBuffer: Uint8Array) {
    this.recordHeader = recordHeader;
    this.pageBuffer = pageBuffer;
    this.keys = [];
    const view = new DataView(this.pageBuffer.buffer)
    const recordBodyAddr = this.recordHeader.addr + this.recordHeader.headerSize;
    let currOffset = 0;
    for (let i = 0; i < this.recordHeader.serialTypes.length; i++) {
      const { length } = Table.getSizeFromSerialType(this.recordHeader.serialTypes[i]);
      // add parsed value into keys, and length to currOffset, convert byte into value
      const serialType = this.recordHeader.serialTypes[i];
      const valueOffset = recordBodyAddr + currOffset;
      const value = Table.parseSQLiteField(view, recordBodyAddr + currOffset, serialType);
      this.keys.push(value)
      currOffset += length;
    }

  }
}