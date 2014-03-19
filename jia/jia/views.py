import binascii
import os
import sys

from flask import redirect
from flask import request
from flask import render_template
from pykronos import KronosClient

from jia import app
from jia.decorators import json_endpoint
from jia.errors import PyCodeError
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
@json_endpoint
def board(id=None):
  if request.method == 'POST':
    board = Board(id=binascii.b2a_hex(os.urandom(5)))
    board.save()
  else:
    board = Board.query.filter_by(id=id).first_or_404()

  return board.json()


@app.route('/pycode', methods=['POST'])
@app.route('/pycode/<id>', methods=['GET', 'PUT'])
@json_endpoint
def pycode(id=None):
  if request.method == 'POST':
    board = Board.query.filter_by(id=request.get_json()['board']).first_or_404()
    pycode = PyCode(board=board)
    pycode.save()
  else:
    pycode = PyCode.query.filter_by(id=id).first_or_404()

  if request.method != 'GET':
    pycode.code = request.get_json().get('code')
    pycode.refresh_seconds = request.get_json().get('refresh_seconds')
    pycode.save()
  
  locals_dict = {
    'kronos_client': KronosClient(app.config['KRONOS_URL'],
                                  namespace=app.config['KRONOS_NAMESPACE']),
    'events': []
    }
  if pycode.code:
    try:
      exec pycode.code in {}, locals_dict # No globals.
    except:
      _, exception, tb = sys.exc_info()
      raise PyCodeError(exception, tb)

  # Don't use [] accessor. `pycode.code` might contain `del response`.
  events = sorted(locals_dict.get('events', []),
                  key=lambda event: event['@time'])

  response = pycode.json()
  response['events'] = events
  return response
