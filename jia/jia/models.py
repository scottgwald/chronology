from jia import db


class Board(db.Model):
  id = db.Column(db.String, primary_key=True)

  def save(self):
    db.session.add(self)
    db.session.commit()


class PyCode(db.Model):
  id = db.Column(db.Integer, autoincrement=True, primary_key=True)
  board_id = db.Column(db.String, db.ForeignKey('board.id'))
  board = db.relationship('Board', backref=db.backref('pycodes',
                                                      lazy='dynamic'))
  code = db.Column(db.Text, nullable=True)

  def save(self):
    db.session.add(self)
    db.session.commit()
