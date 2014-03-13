import binascii
import os

from flask import jsonify
from flask import redirect
from flask import request
from flask import render_template
from pykronos import KronosClient

from jia import app
from jia.models import Board
from jia.models import PyCode


@app.route('/', methods=['GET'])
@app.route('/<board_id>', methods=['GET'])
def index(board_id=None):
  if not board_id:
    # Create a new board and redirect to it.
    board = Board(id=binascii.b2a_hex(os.urandom(5)))
    board.save()
    return redirect('/%s' % board.id, code=302)
  board = Board.query.filter_by(id=board_id).first_or_404()
  return render_template('board.html', board=board)


@app.route('/board', methods=['POST'])
@app.route('/board/<id>', methods=['GET', 'PUT'])
def board(id=None):
  if request.method == 'POST':
    board = Board(id=binascii.b2a_hex(os.urandom(5)))
    board.save()
  else:
    board = Board.query.filter_by(id=id).first_or_404()

  pycode = board.pycodes.first() or PyCode()
  return jsonify({
    'id': board.id,
    'pycode': {'id': pycode.id, 'code': pycode.code}
    })


@app.route('/pycode', methods=['POST'])
@app.route('/pycode/<id>', methods=['GET', 'PUT'])
def pycode(id=None):
  if request.method == 'POST':
    board = Board.query.filter_by(id=request.get_json()['board']).first_or_404()
    pycode = PyCode(board=board)
    pycode.save()
  else:
    pycode = PyCode.query.filter_by(id=id).first_or_404()

  if request.method != 'GET':
    pycode.code = request.get_json().get('code')
    pycode.save()
  
  locals_dict = {
    'kronos_client': KronosClient(app.config['KRONOS_URL'],
                                  namespace=app.config['KRONOS_NAMESPACE']),
    'events': []
    }
  if pycode.code:
    # TODO(usmanm): Do error handling.
    exec pycode.code in {}, locals_dict # No globals.

  # Don't use [] accessor. `pycode.code` might contain `del response`.
  events = sorted(locals_dict.get('events', []),
                  key=lambda event: event['@time'])

  return jsonify({
    'id': pycode.id,
    'board': pycode.board.id,
    'code': pycode.code or '',
    'events': events
    })
