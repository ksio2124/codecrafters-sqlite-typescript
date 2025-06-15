import {Database} from './Database'
const args = process.argv;
const databaseFilePath: string = args[2];
const command: string = args[3];

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
  const table = database.tables.filter((table) => table.name === tableName)[0];
  console.log(await table.getNumberOfRows());
} else if (parsed[0].toUpperCase() === "SELECT" && parsed.at(-2)?.toUpperCase() === "FROM") {
  const regex = /SELECT\s+(.+?)\s+FROM\s+(\w+)/i;
  const match = command.match(regex);
  const columns = (match?.[1] ?? '').split(',').map(s => s.trim());
  const tableName = match?.[2];
  const tables = database.tables.filter(table => table.name === tableName);
  const rows = await tables[0].getAllRows();

  rows.forEach(row => {
    const output: string[] = [];
    columns.forEach(colName => output.push(`${row.content[colName]}`))
    console.log(output.join('|'))
  });
}else {
  throw new Error(`Unknown command ${command}`);
}



await database.close();
