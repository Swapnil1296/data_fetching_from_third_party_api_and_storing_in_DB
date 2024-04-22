const axios = require('axios');
const { Transform } = require('stream');
const db = require('../../configuration/dbConn');
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const moment = require('moment');
const dateFormat = require("dateformat");
const common = require("../../controllers/common");
const current_date = dateFormat(common.currentDateTime(), "yyyy-mm-dd HH:MM:ss")
function checkDate(date) {
	if (date == undefined || date == '' || date == null || date == '00.00.0000') {
		return null;
	} else {
		if (date.match(/^\d{2}-\d{2}-\d{4}$/)) {
			return date.split('-').reverse().join('-');
		} else if (date.match(/^\d{2}.\d{2}.\d{4}$/)) {
			return date.split('.').reverse().join('-');
		} else if (date.match(/^\d{4}.\d{2}.\d{2}$/)) {
			return date.split('.').join('-');
		} else if (date.match(/^\d{4}-\d{2}-\d{2}$/)) {
			return date;
		}
	}
}
function convertToLowerCase(email) {
	if (email && typeof email === 'string' && email.trim() !== '') {
		return email.toLowerCase();
	} else {
		return null;
	}
}

const apiUrl =
	'http://fioriprod.alkemlabs.com:8000/sap/opu/odata/sap/ZSDAPI_SRV/PharmaHeaderSet?expand=PlantNav,SpartNav,LocationNav,DivisionNav,CustomerNav$format=json';
const username = 'SD_KYC';
const password = 'YwKv#29215$';
const batchSize = 100;
const jsonData = {
	Loc: '',
	Div: '',
	Cust: 'X',
	PlantNav: [
	],
	SpartNav: [
	],
	LocationNav: [{}],
	DivisionNav: [{}],
	CustomerNav: [{}],
};


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



const batchInsertDataToDatabase = async (batch) => {
	try {
		await db.none('TRUNCATE TABLE alk_customer_tmp');

		batch.map(async (jsonString, index) => {
			const tmp = JSON.parse(jsonString);
			if (/^[0-9]+$/.test(tmp['CustomerCode']) && tmp["DistributionChannel"] == 10) {
				const obj = {
					customer_code: tmp['CustomerCode'] ? String(tmp['CustomerCode']).replace(/^0+/, '') : tmp['CustomerCode'],
					customer_name: String(tmp['CustomerName']).trim(),
					cfa_code: tmp['CfaCode'],
					location_name: tmp['LocationName'],
					division_code: tmp['DivisionCode'] ? String(tmp['DivisionCode']).replace(/^0+/, '') : tmp['DivisionCode'],
					address_1: tmp['Address1'],
					address_2: tmp['Address2'],
					area: tmp['Area'],
					city: tmp['City'],
					state: tmp['State'],
					pincode: tmp['Pincode'] ? tmp['Pincode'] : null,
					telephone: tmp['Telephone'],
					email: convertToLowerCase(tmp['Email']),
					cst_gst_number: tmp['CstGstNumber'],
					dl_number: tmp['DlNumber'],
					dl_valid_upto: checkDate(tmp['DlValidUpto']),
					pan_number: tmp['PanNumber'],
					credit_days: tmp['CreditDays'] ? tmp['CreditDays'] : null,
					credit_limit: tmp['CreditLimit'] ? tmp['CreditLimit'] : null,
					category_a_b_c: tmp['CategoryABC'],
					locked: tmp['Locked'] ? tmp['Locked'] : null,
					hq: tmp['Hq'] ? String(tmp['Hq']).replace(/^0+/, '') : null,
					hq_name: tmp['HqName'],
				};
				if (
					obj.customer_code &&
					obj.customer_code !== null &&
					obj.customer_code !== '' &&
					obj.cfa_code !== null &&
					obj.cfa_code !== '' &&
					obj.division_code !== null &&
					obj.division_code != '' 
					// &&
					// obj.email !== null &&
					// obj.email !='' &&
					// obj.hq !== null &&
					// obj.hq != ''
				) {
					try {
            
						await db.one(
							'INSERT INTO alk_customer_tmp(customer_code,customer_name,cfa_code,location_name,division_code,address_1,address_2,area,city,state,pincode,telephone,email,cst_gst_number,dl_number,dl_valid_upto,pan_number,credit_days,credit_limit,category_a_b_c,locked,hq,hq_name) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23) RETURNING customer_code',
							[
								obj.customer_code,
								obj.customer_name,
								obj.cfa_code,
								obj.location_name,
								obj.division_code,
								obj.address_1,
								obj.address_2,
								obj.area,
								obj.city,
								obj.state,
								obj.pincode,
								obj.telephone,
								obj.email,
								obj.cst_gst_number,
								obj.dl_number,
								obj.dl_valid_upto,
								obj.pan_number,
								obj.credit_days,
								obj.credit_limit,
								obj.category_a_b_c,
								obj.locked,
								obj.hq,
								obj.hq_name,
							]
						);
					} catch (err) {
						console.log('err:====>', err);
						//common.logError(err);
						return null;
					}
				}
			}
		});
		
		
	} catch (error) {
		console.error('Error truncating table:', error);
		common.customErrorMessage("Error During Insert Tem Database");
	}
};

async function fetchDataAndStoreInDatabase() {
	try {
		const response = await axios.post(apiUrl, jsonData, {
			headers: {
				'Content-Type': 'application/json',
				Authorization: `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`,
				'X-Requested-With': 'XMLHttpRequest',
				Accept: 'application/json',
			},
		});

		const transformerStream = new DataTransformer();
		const dataToProcess = response.data.d.CustomerNav.results;
		console.log("total-data",dataToProcess.length)
		await db
		.one(
		  "INSERT INTO tbl_data_processing(file_name,process_status,process_date,swap_status,file_category,file_created_date,read_status) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id",
		  ['FTP_Pharmarack_Customer API', 1, current_date, 0, "C", current_date, 0, ]
		)
		
		dataToProcess.forEach((item) => {
			const stringItem = JSON.stringify(item);
			transformerStream.write(stringItem);
		});
		transformerStream.end();

		let batch = [];
		transformerStream.on('data', (data) => {
			batch.push(data);
			if (batch.length >= batchSize) {
				batchInsertDataToDatabase(batch);
				batch = [];
			}
		});

		transformerStream.on('end', async () => {
			if (batch.length > 0) {
				batchInsertDataToDatabase(batch).then(async ()=>{
         			 await all_customer_swap();
			
       			 })
			}
			 let check_file_reader_exist = await db.any(
				"Select id,file_name from tbl_data_processing where process_status = 1 AND read_status = 0 AND swap_status = 0 AND file_category='C'"
			  );
			  if(check_file_reader_exist !=""){
				await db
			    .any(
			      `update tbl_data_processing set read_date = $1, read_status= 1 where id = $2 `,
			      [current_date, check_file_reader_exist[0].id]
			    )
			  }
		})

		transformerStream.on('error', (error) => {
			console.error('Error during data transformation:', error);
		});
	} catch (error) {
		console.error('Error:fetchDataAndStoreInDatabase', error);
		common.customErrorMessage("Error During API processing");
	}
}

const all_customer_swap = async () => {
	try {
		await db.tx(async (transaction) => {
			await transaction.none(`
                      INSERT INTO alk_customer (
                          customer_code, customer_name, cfa_code, location_name, division_code, address_1, address_2,
                          area, city, state, pincode, telephone, email, cst_gst_number, dl_number, dl_valid_upto,
                          pan_number, credit_days, credit_limit, category_a_b_c, locked, hq, hq_name
                      )
                      SELECT
                          customer_code, customer_name, cfa_code, location_name, division_code, address_1, address_2,
                          area, city, state, pincode, telephone, email, cst_gst_number, dl_number, dl_valid_upto,
                          pan_number, credit_days, credit_limit, category_a_b_c, locked, hq, hq_name
                      FROM alk_customer_tmp
                      ON CONFLICT (customer_code, cfa_code, division_code)
                      DO UPDATE
                      SET
                          customer_name = EXCLUDED.customer_name,
                          cfa_code = EXCLUDED.cfa_code,
                          location_name = EXCLUDED.location_name,
                          division_code = EXCLUDED.division_code,
                          address_1 = EXCLUDED.address_1,
                          address_2 = EXCLUDED.address_2,
                          area = EXCLUDED.area,
                          city = EXCLUDED.city,
                          state = EXCLUDED.state,
                          pincode = EXCLUDED.pincode,
                          telephone = EXCLUDED.telephone,
                          email = EXCLUDED.email,
                          cst_gst_number = EXCLUDED.cst_gst_number,
                          dl_number = EXCLUDED.dl_number,
                          dl_valid_upto = EXCLUDED.dl_valid_upto,
                          pan_number = EXCLUDED.pan_number,
                          credit_days = EXCLUDED.credit_days,
                          credit_limit = EXCLUDED.credit_limit,
                          category_a_b_c = EXCLUDED.category_a_b_c,
                          locked = EXCLUDED.locked,
                          hq = EXCLUDED.hq,
                          hq_name = EXCLUDED.hq_name;
                  `);
			await transaction.none(`
                      DELETE FROM alk_customer
                      WHERE NOT EXISTS (
                          SELECT 1
                          FROM alk_customer_tmp
                          WHERE alk_customer_tmp.customer_code = alk_customer.customer_code
                              AND alk_customer_tmp.cfa_code = alk_customer.cfa_code
                              AND alk_customer_tmp.division_code = alk_customer.division_code
                      );
                  `);
		}).then( async ()=>{
			console.log("=================,all_customer_swap")
			const check_file_swap_exist = await db.any(
				"SELECT id, file_name FROM tbl_data_processing WHERE process_status = 1 AND read_status = 1 AND swap_status = 0 AND file_category = 'C'"
			  );
			  if (check_file_swap_exist.length > 0) {
				await db.none(
					`UPDATE tbl_data_processing SET swap_date = $1, swap_status = 1 WHERE id = $2`,
					[current_date, check_file_swap_exist[0].id]
				  );
			  }
		})
	} catch (error) {
		console.log('479::error=====>', error);
		common.customErrorMessage("Error During Datas Swap");
		throw error;
	}
};

fetchDataAndStoreInDatabase();
