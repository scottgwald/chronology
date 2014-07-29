from __future__ import absolute_import
from scheduler import db

class Task(db.Model):
  """Scheduled Task

  id: randomly generated string
  active: whether or not the task is active
  code: a Python blob to be `exec`ed
  interval: number of seconds to pause between executions

  Tasks are automatically deactivated if there is another Task running with a
  smaller or equal interval.
  """

  id = db.Column(db.String, primary_key=True)
  active = db.Column(db.Boolean)
  code = db.Column(db.Text)
  interval = db.Column(db.Integer)

  def __repr__(self):
    return '<Task %r>' % self.id

  def save(self):
    db.session.add(self)
    db.session.commit()

  def delete(self):
    db.session.delete(self)
    db.session.commit()
