from jia import app

app.config.from_pyfile('../settings.cfg')
app.run(host='0.0.0.0', port=8151)
