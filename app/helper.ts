export function decodeVarint(bytes: Uint8Array) {
  let result = 0;
  for (let i = 0; i < bytes.length && i < 5; i++) {
    result = (result << 7) | (bytes[i] & 0x7F);
    if ((bytes[i] & 0x80) === 0) {
      return result;
    }
  }
}

export function parseSQLiteVarints32(buffer: Uint8Array) {
  const view = new DataView(buffer.buffer);
  const result = [];
  let offset = 0;
  // console.log('bytelength', buffer.byteLength)
  // console.log(buffer);
  while (offset < buffer.byteLength) {
    let value = 0;
    let bytesRead = 0;

    for (let i = 0; i < 9; i++) {
      if (offset + i >= buffer.byteLength) break;
      const byte = view.getUint8(offset + i);
      bytesRead++;

      if (i < 8) {
        value = (value << 7) | (byte & 0x7F);
        if ((byte & 0x80) === 0) break; // last byte
      } else {
        // 9th byte: all 8 bits are data
        value = (value << 8) | byte;
        break;
      }
    }

    result.push({ value, offset, bytesRead });
    // result.push(value)
    offset += bytesRead;
  }

  return result;
}
