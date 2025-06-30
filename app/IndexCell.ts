import { parseSQLiteVarints32 } from "./helper";
import { Payload } from "./Payload";

const LEFT_CHILD_POINTER_BYTE_LENGTH = 4

export class IndexInteriorCell {
  addr: number;
  pageBuffer: Uint8Array;
  pageView: DataView;
  payload: Payload
  constructor(addr: number, pageBuffer: Uint8Array) {
    this.addr = addr;
    this.pageBuffer = pageBuffer
    this.pageView = new DataView(pageBuffer.buffer)
    const { value: payloadSize, bytesRead: payloadSizeLength } = this.getPayloadSize();
    const payloadAddr = addr + LEFT_CHILD_POINTER_BYTE_LENGTH + payloadSizeLength;
    this.payload = new Payload(this.pageBuffer, payloadAddr)
  }

  getLeftChildPageNum() {
    return this.pageView.getUint32(this.addr)
  }

  getPayloadSize() {
    // max varint is 9 bit rowid, payloadSize
    const temp = this.pageBuffer.slice(this.addr + LEFT_CHILD_POINTER_BYTE_LENGTH, this.addr + LEFT_CHILD_POINTER_BYTE_LENGTH + 9)
    return parseSQLiteVarints32(temp)[0];
  }
}

export class IndexLeafCell {
  addr: number;
  pageBuffer: Uint8Array;
  pageView: DataView;
  payload: Payload
  constructor(addr: number, pageBuffer: Uint8Array) {
    this.addr = addr;
    this.pageBuffer = pageBuffer
    this.pageView = new DataView(pageBuffer.buffer)
    const { value: payloadSize, bytesRead: payloadBytesRead } = this.getPayloadSize();
    this.payload = new Payload(this.pageBuffer, addr + payloadBytesRead)
  }


  getPayloadSize() {
    // max varint is 9 bit rowid, payloadSize
    const temp = this.pageBuffer.slice(this.addr, this.addr + 9)
    return parseSQLiteVarints32(temp)[0];
  }
}