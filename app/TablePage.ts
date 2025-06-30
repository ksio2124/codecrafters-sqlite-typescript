import { CELL_PTR_LENGTH, FRAGMENTED_FREE_BYTE, NUMBER_OF_CELL, RIGHT_MOST_POINTER } from "./const"
import { TableInteriorCell, TableLeafCell } from "./TableCell"

export class TableInteriorPage {
  pageBuffer: Uint8Array
  pageView: DataView
  constructor(pageBuffer: Uint8Array) {
    this.pageBuffer = pageBuffer
    this.pageView = new DataView(this.pageBuffer.buffer)
  }
  getCellCount() {
    return this.pageView.getUint16(NUMBER_OF_CELL.offset)
  }

  getCellPtrs() {
    const cellAddresses = [];
    const cellPtrsStart = RIGHT_MOST_POINTER.offset + RIGHT_MOST_POINTER.size;
    for (let idx = 0; idx < this.getCellCount(); idx++) {
      const cellAddress = this.pageView.getUint16(cellPtrsStart + CELL_PTR_LENGTH * idx)
      cellAddresses.push(cellAddress)
    }
    return cellAddresses
  }

  getCells() {
    return this.getCellPtrs().map((cellPtr) => new TableInteriorCell(cellPtr, this.pageBuffer))
  }

  getRightMostPageNum() {
    return this.pageView.getUint32(RIGHT_MOST_POINTER.offset)
  }
}

export class TableLeafPage {
  pageBuffer: Uint8Array
  pageView: DataView
  constructor(pageBuffer: Uint8Array) {
    this.pageBuffer = pageBuffer
    this.pageView = new DataView(this.pageBuffer.buffer)
  }
  getCellCount() {
    return this.pageView.getUint16(NUMBER_OF_CELL.offset)
  }

  getCellPtrs() {
    const cellAddresses = [];
    const cellPtrsStart = FRAGMENTED_FREE_BYTE.offset + FRAGMENTED_FREE_BYTE.size
    for (let idx = 0; idx < this.getCellCount(); idx++) {
      const cellAddress = this.pageView.getUint16(cellPtrsStart + CELL_PTR_LENGTH * idx)
      cellAddresses.push(cellAddress)
    }
    return cellAddresses
  }

  getCells() {
    return this.getCellPtrs().map((cellPtr) => new TableLeafCell(cellPtr, this.pageBuffer))
  }
}