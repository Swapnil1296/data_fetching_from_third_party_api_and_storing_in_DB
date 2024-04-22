const Promise = require("bluebird");
const Config = require("./config");
//const common = require("../controllers/common");
const initOptions = {
  promiseLib: Promise,
  query(e) {
 console.log("+++++++++++++++++++");
 console.log( e.query);
 console.log("+++++++++++++++++++");
  },
  error(error, e) {
    if (e.cn) {
      // A connection-related error;
      // Connections are reported back with the password hashed,
      // for safe errors logging, without exposing passwords.
      console.log("CN:", e.cn);
      console.log("EVENT:", error.message || error);
      var toArr = [
        //  "soumyadeep@indusnet.co.in",
        "inderjit.singh@indusnet.co.in",
        "soumen.maity@indusnet.co.in",
      ];
      common.sendMail({
        from: Config.webmasterMail, // sender address
        to: toArr, // list of receivers
        subject: `URL ||  ${Config.website.backend_url} || DB Error`, // Subject line
        html: `Error: ${JSON.stringify(e.cn)} <br> ${JSON.stringify(
          error.message
        )} <br> ${JSON.stringify(error)}`, // plain text body
      });
    }
  },
};
const pgp = require("pg-promise")(initOptions);

const cn = {
  host: Config.db.DB_HOST, // 'localhost' is the default;
  port: Config.db.DB_PORT, // 5432 is the default;
  database: Config.db.DB_NAME,
  user: Config.db.DB_USER,
  password: Config.db.DB_PASS,
};
// const cn = 'postgres://process.env.DB_USER:process.env.DB_PASS@process.env.DB_HOST:process.env.DB_PORT/process.env.DB_NAME';

pgp.pg.types.setTypeParser(1114, (s) => s);

const db = pgp(cn); // database instance;

db.connect()
  .then((obj) => {
    obj.done();
  })
  .catch((error) => {
    console.log("ERROR:", error.message || error);
  });

module.exports = db;
