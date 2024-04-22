const promise = require("bluebird");

const initOptions = {
  promiseLib: Promise,
  query(e) {
    // console.log("+++++++++++++++++++");
    // console.log(e.query);
    // console.log("+++++++++++++++++++");
  },
  error(error, e) {
    if (e.cn) {
      // A connection-related error;
      // Connections are reported back with the password hashed,
      // for safe errors logging, without exposing passwords.
      console.log("CN:", e.cn);
      console.log("EVENT:", error.message || error);
    }
  },
};
const pgp = require("pg-promise")(initOptions);
// Create a PostgreSQL client instance
const client = {
  user: "postgres",
  host: "localhost",
  database: "postgres",
  password: "Swapy@1996",
  port: 5432,
};

pgp.pg.types.setTypeParser(1114, (s) => s);
// Connect to the PostgreSQL database
const db = pgp(client);
db.connect()
  .then(() => {
    console.log("Connected to PostgreSQL");
    // Perform database operations here
  })
  .catch((err) => {
    console.error("Error connecting to PostgreSQL:", err);
  });
module.exports = db;
