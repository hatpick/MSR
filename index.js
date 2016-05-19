var mysql = require('mysql');
var moment = require('moment');
var config = require('./config');
var fs = require('fs');

var RESULT = {'COMMIT':[], 'PULL_REQUEST':[]};
var COUNT = 0;

function calculateDistanceInDays(d1, d2) {
	var oneDay = 24*60*60*1000;
	return Math.round(Math.abs((d1.getTime() - d2.getTime())/(oneDay)));
}

var PROJECT_ID = '27504', OWNER_ID = '81111';
var DATE = '{D}';
var DATEDATE = '{DD}';
var DATE_COL = 'created_at';
var WEEK_INTERVAL_STATEMENT = '{DD}';
var CQUERY ='select count(distinct(commits.committer_id)) as USERS' +
			' from commits' +
			' where id not in (select base_commit_id from pull_requests where base_repo_id=' + PROJECT_ID + ')' +
			' AND project_id=' + PROJECT_ID +			
			' AND ' + DATE_COL +' >= {D}' +
			' AND ' + DATE_COL + ' < ' + WEEK_INTERVAL_STATEMENT;

var PQUERY ='select count(distinct(commits.committer_id)) AS USERS' +
			' from commits inner join pull_requests on commits.id = pull_requests.base_commit_id' +
			' where commits.project_id =' + PROJECT_ID +			
			' AND commits.' + DATE_COL +' >= {D}' + 
			' AND commits.' + DATE_COL + ' < ' + WEEK_INTERVAL_STATEMENT;

var rip_date = 'select * from commits where project_id = ' + PROJECT_ID + ' order by created_at desc';
var birth_date = 'select * from commits where project_id = ' + PROJECT_ID + ' order by created_at';

var connection = mysql.createConnection({
  host     : config.SERVER_ADDRESS,  
  user     : config.USERNAME,
  password : config.PASSWORD,
  database : config.DB_NAME
});

connection.connect(function(err) {
	if(err) {
		console.log(err);
	}
});

var bd, rip;
connection.query(birth_date, function(err, rows, fields) {
	if (err) throw err;
	bd = new Date(rows[0].created_at);
});

connection.query(rip_date, function(err, rows, fields) {
	if (err) throw err;
	rip = new Date(rows[0].created_at);
	var count = calculateDistanceInDays(bd, rip);	
	COUNT = Math.round(count/7);
	var dString = moment(bd).format('YYYY-MM-DD 00:00:00');
	var dString1 = moment(bd).add(7, 'days').format('YYYY-MM-DD 00:00:00');
	var query;
	for(var i = 0; i < count; i += 7) {						
		query = CQUERY.replace(DATE, '\'' + dString + '\'').replace(DATEDATE, '\'' + dString1 + '\'');
		var d = moment(dString).add(7, 'days');
		dString = d.format('YYYY-MM-DD 00:00:00');
		dString1 = moment(d).add(7, 'days').format('YYYY-MM-DD 00:00:00');		
		(function(q){
			connection.query(q, function(err, rows, fields) {				
				if(err) throw err;								
				var c = rows[0].USERS;				
				//console.log(q);
				RESULT['COMMIT'].push(c);
			});
		})(query);
	}	

	dString = moment(bd).format('YYYY-MM-DD 00:00:00');
	dString1 = moment(bd).add(7, 'days').format('YYYY-MM-DD 00:00:00');	

	for(var i = 0; i < count; i += 7) {						
		query = PQUERY.replace(DATE, '\'' + dString + '\'').replace(DATEDATE, '\'' + dString1 + '\'');
		var d = moment(dString).add(7, 'days');
		dString = d.format('YYYY-MM-DD 00:00:00');
		dString1 = moment(d).add(7, 'days').format('YYYY-MM-DD 00:00:00');
		(function(q, d){			
			connection.query(q, function(err, rows, fields) {								
				if(err) throw err;					
				var c = rows[0].USERS;				
				//console.log(q);
				RESULT['PULL_REQUEST'].push(c);
				if(d == '2013-09-25 00:00:00') {					
					toCSV(RESULT);
					connection.end();
				}
			});
		})(query, dString1);
	}
});

function toCSV(obj) {		
	var COMMIT_COL = "commits";
	var PULL_REQUEST_COL = "pull_requests";
	var ws = fs.createWriteStream('./results.csv');
	fs.appendFileSync('./msr-results.csv', COMMIT_COL + ', ' + PULL_REQUEST_COL + '\n');
	for(var i = 0 ; i < COUNT; i++) {
		fs.appendFileSync('./msr-results.csv', obj['COMMIT'][i] + ', ' + obj['PULL_REQUEST'][i] + '\n');
	}	
	ws.end();
}
