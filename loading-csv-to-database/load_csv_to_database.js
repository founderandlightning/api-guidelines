const fs = require('fs');
const csv = require('fast-csv');

/* Connection string got from envs is recommended */
const knex = require('knex')({
  client: 'mysql',
  connection: {
    host: '127.0.0.1',
    user: 'your_database_user',
    password: 'your_database_password',
    database: 'myapp_test',
  },
});

const STEP = 500;

module.exports = async function loadCSVToDatabase(
  filename,
  csvColumnsWithTypes,
  databaseColumns,
  { tableName, uniqueIndex, transformChunks } = {},
) {
  if (!tableName) {
    throw new Error('You must specify destination table, field: tableName');
  }
  if (!uniqueIndex) {
    throw new Error('You must specify unique index name, field: uniqueIndex');
  }

  const saveData = async function (data) {
    if (transformChunks) {
      data.forEach((chunk) => {
        transformChunks(chunk, knex);
      });
    }

    try {
      await knex('temp_table_to_load_csv').insert(data);
    } catch (e) {
      console.log(e);
    }
  };

  try {
    const csvHeaders = csvColumnsWithTypes.map(column => column.split(' ')[0]);
    const stream = fs.createReadStream(filename);

    /* When we stop script during execution, there will be temp_table with data in the db. We need to delete them */
    await knex.raw('DROP TABLE IF EXISTS temp_table_to_load_csv');
    /* Create table based on *** */
    await knex.raw(`CREATE TABLE IF NOT EXISTS temp_table_to_load_csv (
      ${csvColumnsWithTypes}
    )`);

    const csvStream = csv.fromStream(stream, { headers: csvHeaders });

    const data = [];
    let saves = 0;
    let header = true;

    for await (const chunk of csvStream) {
      if (!header) {
        data.push(chunk);

        if (data.length === STEP) {
          await saveData(data);
          data.length = 0;
          saves += 1;
        }
      }
      header = false;
    }

    // This is needed for the last batch of data
    // that has less than ${STEP} items
	console.log('saving', saves);
    await saveData(data);

    console.log(`Inserting rows into ${tableName}`);

    /* eslint-disable indent */
    await knex.raw(`
      INSERT INTO ${tableName} (
        ${databaseColumns.join(',')}
      ) (
        SELECT ${csvHeaders.join(',')}
        FROM temp_table_to_load_csv
      )
      ON CONFLICT ON CONSTRAINT ${uniqueIndex} DO UPDATE SET
      ${
        databaseColumns
          .map(column => `${column} = EXCLUDED.${column}`)
          .join(',')
      }
    `);
    /* eslint-enable indent */

    console.log('Removing temp table');
    await knex.raw(`
      DROP TABLE IF EXISTS temp_table_to_load_csv
    `);

    console.log('Finished');
    console.log('Saved', saves * STEP + data.length);
    process.exit(0);
  } catch (e) {
    await knex.raw(`
      DROP TABLE IF EXISTS temp_table_to_load_csv
    `);

    console.log(e.message);
    process.exit(0);
  }
};
