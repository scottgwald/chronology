from jia import db


class Board(db.Model):
  id = db.Column(db.String, primary_key=True)

  def save(self):
    db.session.add(self)
    db.session.commit()

  def json(self):
    pycode = self.pycodes.first() or PyCode()
    return {'id': self.id,
            'pycode': pycode.json()}


class PyCode(db.Model):
  id = db.Column(db.Integer, autoincrement=True, primary_key=True)
  board_id = db.Column(db.String, db.ForeignKey('board.id'))
  board = db.relationship('Board', backref=db.backref('pycodes',
                                                      lazy='dynamic'))
  code = db.Column(db.Text, nullable=True)
  refresh_seconds = db.Column(db.Integer, nullable=True)

  def save(self):
    db.session.add(self)
    db.session.commit()

  def json(self):
    board = self.board or Board()
    return {
      'id': self.id,
      'board': board.id,
      'code': self.code or '',
      'refresh_seconds': self.refresh_seconds
      }

