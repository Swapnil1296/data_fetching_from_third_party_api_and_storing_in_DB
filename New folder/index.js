const express = require("express");
const db = require("./confinguration/dbconfi.js");
const axios = require("axios");
const { Transform } = require("stream");
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const moment = require("moment");

const app = express();
const port = 8080;
app.use(express.json());

const formatted_date = Date.now();
const date = new Date(formatted_date);

const current_date = `${date.getFullYear()}-${String(
  date.getMonth() + 1
).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")} ${String(
  date.getHours()
).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}:${String(
  date.getSeconds()
).padStart(2, "0")}`;

function checkDate(date) {
  if (date == undefined || date == "" || date == null || date == "00.00.0000") {
    return null;
  } else {
    if (date.match(/^\d{2}-\d{2}-\d{4}$/)) {
      return date.split("-").reverse().join("-");
    } else if (date.match(/^\d{2}.\d{2}.\d{4}$/)) {
      return date.split(".").reverse().join("-");
    } else if (date.match(/^\d{4}.\d{2}.\d{2}$/)) {
      return date.split(".").join("-");
    } else if (date.match(/^\d{4}-\d{2}-\d{2}$/)) {
      return date;
    }
  }
}
// a function to transform data into chunks
class DataTransformer extends Transform {
  constructor(options) {
    super(options);
  }

  _transform(chunk, encoding, callback) {
    try {
      this.push(chunk); // No need to stringify the chunk
      callback();
    } catch (error) {
      callback(error);
    }
  }
}
// app.get("/api/test", async (req, res) => {
//   fetchDataAndStoreInDatabase().then(() =>
//     res.status(200).json({ message: "swaping done successfully" })
//   );
// });
const truncateExistingData = async () => {
  await db.none("TRUNCATE TABLE alk_api_tmp");
};
truncateExistingData();
const batchInsertDataToDatabase = async (batch) => {
  try {
    batch.map(async (jsonstring, index) => {
      const tmp = JSON.parse(jsonstring);

      if (/^[0-9]+$/.test(tmp["userId"])) {
        const obj = {
          user_id: tmp["userId"]
            ? String(tmp["userId"]).replace(/^0+/, "")
            : tmp["userId"],
          id: tmp["id"] ? String(tmp["id"]).replace(/^0+/, "") : tmp["id"],
          title: String(tmp["title"]).trim(),
          body: String(tmp["body"]).trim(),
        };
        {
          try {
            await db.one(
              "INSERT INTO alk_api_tmp(user_id,id,title,body) VALUES ($1,$2,$3,$4) RETURNING user_id",
              [obj.user_id, obj.id, obj.title, obj.body]
            );
          } catch (err) {
            console.log("err:====>", err);

            return null;
          }
        }
      }
    });
  } catch (error) {
    console.error("Error truncating table:", error);
  }
};
const fetchDataAndStoreInDatabase = async () => {
  try {
    const response = await axios.get(
      "https://jsonplaceholder.typicode.com/posts"
    );
    const posts = response.data;
    const transformerStream = new DataTransformer();
    const dataToProcess = posts;
    await db.one(
      "INSERT INTO tbl_processing_data_status(file_name,process_status,process_date,swap_status,file_category,file_created_date,read_status) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id",
      ["FTP_Pharmarack_Customer API", 1, current_date, 0, "C", current_date, 0]
    );
    dataToProcess.forEach((item) => {
      const stringItem = JSON.stringify(item);

      transformerStream.write(stringItem);
    });
    transformerStream.end();
    let batch = [];
    const batchSize = 10;
    transformerStream.on("data", (data) => {
      batch.push(data);
      if (batch.length >= batchSize) {
        batchInsertDataToDatabase(batch);

        batch = [];
      }
    });

    transformerStream.on("end", async () => {
      if (batch.length >= 0) {
        batchInsertDataToDatabase(batch).then(async () => {
          console.log("one");
          setTimeout(async () => {
            await all_customer_swap();
            console.log("two");
          }, 15000);

          console.log("three");
        });
      }
      let check_file_reader_exist = await db.any(
        "Select id,file_name from tbl_processing_data_status where process_status = 1 AND read_status = 0 AND swap_status = 0 AND file_category='C'"
      );
      if (check_file_reader_exist != "") {
        await db.any(
          `update tbl_processing_data_status set read_date = $1, read_status= 1 where id = $2 `,
          [current_date, check_file_reader_exist[0].id]
        );
      }
    });

    transformerStream.on("error", (error) => {
      console.error("Error during data transformation:", error);
    });
  } catch (error) {
    console.log(error);
  }
};
const all_customer_swap = async (req, res) => {
  try {
    await db
      .tx(async (transaction) => {
        await transaction.none(`
                     INSERT INTO alk_api (user_id, id, title, body)
SELECT
    user_id, id, title, body
FROM 
    alk_api_tmp
ON CONFLICT (user_id, id)
DO UPDATE SET
    title = EXCLUDED.title,
    body = EXCLUDED.body;
                         
                  `);
        // it will check if any user is already existing in alk_api table but when new data comes
        // in alk_api_tmp table it will validate that the user is exist in the new data also , if
        // the user does not exist in new data (alk_api_tmp table) it will delete the user from main table.
        await transaction.none(`
                      DELETE FROM alk_api
                      WHERE NOT EXISTS (
                          SELECT 1
                          FROM alk_api_tmp
                          WHERE alk_api_tmp.user_id = alk_api.user_id
                              AND alk_api_tmp.id = alk_api.id

                      );
                  `);
      })
      .then(async () => {
        console.log("=================,all_customer_swap");
        const check_file_swap_exist = await db.any(
          "SELECT id, file_name FROM tbl_processing_data_status WHERE process_status = 1 AND read_status = 1 AND swap_status = 0 AND file_category = 'C'"
        );
        if (check_file_swap_exist.length > 0) {
          await db.none(
            `UPDATE tbl_processing_data_status SET swap_date = $1, swap_status = 1 WHERE id = $2`,
            [current_date, check_file_swap_exist[0].id]
          );
        }
      });
  } catch (error) {
    console.log("479::error=====>", error);
    // common.customErrorMessage("Error During Datas Swap");
    throw error;
  }
};
// app.get("/api/test", async (req, res) => {
//   try {
//     const response = await axios.get(
//       "https://jsonplaceholder.typicode.com/posts"
//     );
//     const posts = response.data.slice(0, 20);
//     // Create a temporary table with the same structure as alk_api
//     await db.query(`
//       CREATE TEMP TABLE temp_alk_api AS SELECT * FROM alk_api WHERE 1=0;
//     `);
//     // Insert fetched data into the temporary table
//     const values = posts.map((post) => [
//       post.userId,
//       post.id,
//       post.title.replace(/'/g, "''"),
//       post.body.replace(/'/g, "''"),
//     ]);
//     const insertQuery = `
//       INSERT INTO temp_alk_api (user_id, id, title, body)
//       VALUES($1, $2, $3, $4)
//     `;
//     await Promise.all(values.map((value) => db.query(insertQuery, value)));
//     // Compare and upsert data into alk_api
//     const upsertQuery = `
//       INSERT INTO alk_api (user_id, id, title, body)
//       SELECT t.user_id, t.id, t.title, t.body
//       FROM temp_alk_api t
//       LEFT JOIN alk_api a ON t.user_id = a.user_id AND t.id = a.id
//       WHERE a.user_id IS NULL OR
//             a.title != t.title OR
//             a.body != t.body
//       ON CONFLICT (user_id, id)
//       DO UPDATE SET
//         title = EXCLUDED.title,
//         body = EXCLUDED.body
//     `;
//     await db.query(upsertQuery);
//     console.log("Data inserted/updated successfully");
//     res.send(posts);
//   } catch (error) {
//     console.log("Error:", error.message);
//     res.status(500).send("Server error");
//   }
// });
fetchDataAndStoreInDatabase();
app.listen(port, function (err) {
  if (err) {
    console.log("Error while starting server:", err.message);
  } else {
    console.log(`Server has been started at ${port}`);
  }
});
