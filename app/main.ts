import { Database } from './Database'
const args = process.argv;
const databaseFilePath: string = args[2];
const command: string = args[3];


function trimMatchingQuotes(str: string) {
  if (
    (str.startsWith('"') && str.endsWith('"')) ||
    (str.startsWith("'") && str.endsWith("'"))
  ) {
    return str.slice(1, -1);
  }
  return str;
}

const database = new Database(databaseFilePath);
await database.init();
await database.findAllTables();
const parsed = command.split(" ");
if (parsed[0] === ".dbinfo") {
  console.log(`database page size: ${database.pageSize}`);
  console.log(`number of tables: ${database.tableSize}`);
} else if (parsed[0] === ".tables") {
  console.log(database.tables.map((table) => table.name).join(" "));
} else if (
  parsed[0].toUpperCase() === "SELECT" &&
  parsed[1].toUpperCase() === "COUNT(*)"
) {
  const tableName = parsed.at(-1);
  console.log('here', tableName, parsed)
  const table = database.tables.filter((table) => table.name === tableName)[0];
  // console.log(await table.getColumnNames(table.sql!));
  console.log((await table.getAllRows()).length)

} else if (parsed[0].toUpperCase() === "SELECT") {
  const regex = /^SELECT\s+(.*?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*))?$/i
  // const regex = /SELECT\s+(.+?)\s+FROM\s+(\w+)/i
  const match = command.match(regex);
  const columns = (match?.[1] ?? '').split(',').map(s => s.trim());
  const tableName = match?.[2];
  const [filterkey, filterVal] = (match?.[3] ?? '').split('=').map((val) => trimMatchingQuotes(val.trim())) ?? [];
  const tables = database.tables.filter(table => table.name === tableName);
  // if there is a index table that matches the look up of tableName and filterKey 
  // console.log(tables.)
  // database.tables.forEach((table) => console.log(table.name))
  const indexTable = database.tables.find((table) => table.name === `idx_${tableName}_${filterkey}`)
  if (indexTable !== undefined) {
    let res = await (indexTable.getMatchingIndex(filterVal));
    const table = database.tables.find((table) => table.name === tableName);
    res.sort((a, b) => a - b);
    const columnNames = table?.getColumnNames(table.sql!)
    // console.log(columnNames)
    const recordBodies = await table?.getRecordOnIds(res);
    recordBodies?.res.forEach((recordBody, recordBodyIdx) => {
      const output: string[] = [];
      // console.log(recordBody.keys)
      columnNames?.forEach((colName, idx) => {
        if (colName === 'id' && columns.includes('id')) {
          output.push(recordBody.keys[idx].value ?? res[recordBodyIdx])
          return;
        }
        if (columns.includes(colName)) {
          output.push(recordBody.keys[idx].value)
        }
      });
      console.log(output.join('|'));
    })
  } else {
    const rows = await tables[0].getAllRows();
    rows.forEach(row => {
      const output: string[] = [];
      if (row.content[filterkey] !== filterVal) {
        return;
      }
      columns.forEach(colName => {
        output.push(`${row.content[colName]}`)
      })
      console.log(output.join('|'))
    });
  }
} else {
  throw new Error(`Unknown command ${command}`);
}



await database.close();
