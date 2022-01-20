const request = require('request');

const express = require('express');
var session = require('cookie-session');
var bodyParser = require('body-parser');

const engines = require('consolidate');
var hbs = require('handlebars');

const dotgit = require('dotgitignore')();
const cors = require('cors')

require('dotenv').config();

var mysql = require('mysql');

const url = require('url');
const fs = require('fs'); 
const app = express();

app.engine('hbs', engines.handlebars);
app.set('views', './views');
app.set('view engine', 'hbs');


const connection  = mysql.createPool({
	host     : 'eu-cdbr-west-01.cleardb.com',
	user     : process.env.DB_USER,
	password :  process.env.DB_PWD,
	database : 'heroku_1b392502415228b'
});


app.use(cors({origin: true}));
app.use(session({
    secret: 'secret',
    resave: true,
    saveUninitialized: true
}));

app.use(bodyParser.urlencoded({extended : true}));
app.use(bodyParser.json());


app.get('/', function (request, response) {
    response.render('login');
});


app.post('/auth', function (request, response){

    var username = request.body.username;
    var password = request.body.password;


	if (username && password) {
		connection.query('SELECT * FROM user WHERE username = ? AND password = ?', [username, password], function(error, results, fields) {
			if (results.length > 0) {

            request.session.loggedin = true;
            request.session.username = username;

            response.render('query');
        } else {
            response.render('error', { error: 'Incorrect Username and/or Password!' });
        }
    });

    } else {
        response.render('error', { error: 'Please enter Username and Password!' });
    };
});

app.get('/query', function (request, response) {  
    if (request.session.loggedin) {
        response.render('query');
    } else {
        response.render('error', { error: 'Please login to view this page!' });
    }
});


app.post('/results', function (request, response) {
    if (request.session.loggedin) {

        var domains = request.body.domains;
        var file = request.body.file;
        var clientDomain = request.body.clientDomain;
        var country = request.body.country;
        var start_month = request.body.start_month;
        var end_month = request.body.end_month;
        var viewId = request.body.viewId;

        

        // Fetch year, month and day of respective dates
        const [sy, sm] = start_month.split('-')
        const [ey, em] = end_month.split('-')

        // Constructing dates from given string date input
        const startDate = new Date(sy, sm, 01)
        const endDate = new Date(ey, em, 01)

        // Validate end date so that it must after start date
        if (endDate <= startDate) {
            response.render('error', { error: "End date must be after start date." });
        }

        const { spawn } = require('child_process');
        obj = { file: file, clientDomain: clientDomain, domains: domains, country: country, start_month: start_month, end_month: end_month, viewId: viewId}
        if ("app" in request.body) {
            obj["app"] = request.body.app;
        }

        const childPython = spawn('python', ['similarweb.py', JSON.stringify(obj)]);


        childPython.stderr.on('data', (data) => {
            console.log(`stderr: ${data}`)
        });

         childPython.stdout.on('data', (data) => {
            var data = JSON.parse(data.toString().replace(/'/g, '"'));
            console.log('stdout', data)
            request.body.status = data['status'];


            if (request.body.status == "success") {
                response.render('output', { path: data['path'] });
            } else {

                response.render('error', { error: data['status'] });
            }
        });


    } else {
        response.render('error', { error: 'Please enter Username and Password!' });
    }
});

const port = process.env.PORT || 3000
app.listen(port)

console.log(`Server is listening on port ${port}`);
