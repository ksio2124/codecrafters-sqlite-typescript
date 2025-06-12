import { open } from 'fs/promises';
import { constants } from 'fs';
import type { FileHandle } from 'fs/promises';

const args = process.argv;
const databaseFilePath: string = args[2]
const command: string = args[3];

const getTableName = async (recordPtr: number, databaseFileHandler: FileHandle): Promise<[number, string]> => {
    let buffer: Uint8Array = new Uint8Array(4);
    await databaseFileHandler.read(buffer, 0, buffer.length, recordPtr);
    const recordSize = new DataView(buffer.buffer, 0, buffer.byteLength).getUint8(0);
    buffer = new Uint8Array(recordSize);
    await databaseFileHandler.read(buffer, 0, buffer.length, recordPtr);
    const recordHeaderSize = new DataView(buffer.buffer, 0, buffer.byteLength).getUint8(2);
    const type = new DataView(buffer.buffer, 0, buffer.byteLength).getUint8(3);
    const name = new DataView(buffer.buffer, 0, buffer.byteLength).getUint8(4);
    const tblName = new DataView(buffer.buffer, 0, buffer.byteLength).getUint8(5);
    const recordBodyPtr = recordHeaderSize + 2;
    const tableNameOffset = ((type - 13) / 2) + ((name - 13) / 2);
    const tableNamePtr = recordBodyPtr + tableNameOffset;
    const tableNameSize = (tblName - 13) / 2
    const test = new DataView(buffer.buffer, tableNamePtr, tableNameSize);
    const decoder = new TextDecoder();
    const tableName = decoder.decode(test);
    // console.log(`table name: ${decoder.decode(test)}`);
    return [recordSize + recordPtr, tableName];
}

if (command === ".dbinfo") {
    const databaseFileHandler = await open(databaseFilePath, constants.O_RDONLY);
    const buffer: Uint8Array = new Uint8Array(105);
    await databaseFileHandler.read(buffer, 0, buffer.length, 0);

    // You can use print statements as follows for0 debugging, they'll be visible when running tests.
    console.error("Logs from your program will appear here!");

    // Uncomment this to pass the first stage    
    const pageSize = new DataView(buffer.buffer, 0, buffer.byteLength).getUint16(16);
    // const numberOfTables = new DataView(
    console.log(`database page size: ${pageSize}`);
    const tableSize = new DataView(buffer.buffer, 0, buffer.byteLength).getUint16(103);
    console.log(`number of tables: ${tableSize}`)

    await databaseFileHandler.close();
} else if (command === '.tables') {
    const databaseFileHandler = await open(databaseFilePath, constants.O_RDONLY);
    let buffer: Uint8Array = new Uint8Array(4);
    await databaseFileHandler.read(buffer, 0, buffer.length, 112);
    const cellPtrArray = new DataView(buffer.buffer, 0, buffer.byteLength).getUint16(0);
    await databaseFileHandler.read(buffer, 0, buffer.length, 103);
    const tableSize = new DataView(buffer.buffer, 0, buffer.byteLength).getUint16(0);
    let newRecordPtr = cellPtrArray
    const tableNames = [];
    let tableName = ''
    for (let i = 0; i < tableSize; i++) {
        [newRecordPtr, tableName] = await getTableName(newRecordPtr, databaseFileHandler);
        newRecordPtr += 2;
        tableNames.push(tableName);
    }
    console.log(tableNames.filter((name) => name !== 'sqlite_sequence').join(' '));
} else {
    throw new Error(`Unknown command ${command}`);
}
