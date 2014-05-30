from jia import db
import json

class Board(db.Model):
  class PanelSource(object):
    PYCODE = 'pycode'

  class PanelDisplay(object):
    TIMESERIES = 'timeseries'
    TABLE = 'table'

  id = db.Column(db.String, primary_key=True)
  # JSON-encoded description of the board of the form { '__version__':
  # 1, ...}, where ... is specified in `Board.json`.
  #
  # `__version__` is the serialization version of the board.  If, in
  # the future, we change the serialization format of boards, we'll be
  # able to read and migrate previously serialized boards.
  board_data = db.Column(db.String)

  def save(self):
    db.session.add(self)
    db.session.commit()

  def json(self):
    """A JSON-encoded description of this board.

    Format:
    {'id': board_id,
     'title': 'The title of the board',
     'panels': [{
       'title': 'The title of the panel'
       'data_source': {
         'source_type': PanelSource.TYPE,
         'refresh_seconds': 600,
         ...source_specific_details...
       },
       'display': {
         'display_type': PanelDisplay.TYPE,
         ...display_specific_details...
       }, ...]}
    """
    if self.board_data:
      board_dict = json.loads(self.board_data)
      board_dict['id'] = self.id
      del board_dict['__version__']
    else:
      board_dict = {
        'id': self.id,
        'title': '',
        'panels': []
        }
    return board_dict
    """    pycode = self.pycodes.first() or PyCode()
    return {'id': self.id,
            'pycode': pycode.json()}
            """
  def set_board_data(self, board_dict):
    assert board_dict['id'] == self.id
    del board_dict['id']
    self.board_data = json.dumps(dict(board_dict.items() +
                                      [('__version__', 1)]))

