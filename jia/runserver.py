from jia import app

if __name__ == '__main__':
  app.config.from_pyfile('../settings.cfg')
  app.run(host='0.0.0.0', port=8153)
