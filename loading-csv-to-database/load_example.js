const loadCSVToDatabase = require('./load_csv_to_database');

loadCSVToDatabase(
  './datasets/example_data.csv',
  [
    'ugly_name VARCHAR', // after space we put the column type
    'some_number NUMERIC',
  ], [
    'pretty_name', // change name from ugly_name to pretty_name
    'some_number',
  ],
  {
    tableName: 'table_name',
    uniqueIndex: 'ugly_name_unique',
    transformChunks(row, knex) {
      // here we can tranform cell
      // for example we want to add random number to name
      row.ugly_name = `${row.ugly_name}${Math.radom()}`;

      // change empty string to null
      Object.keys(row).forEach((key) => {
        row[key] = row[key] === '' ? null : row[key];
      });
    },
  },
);
