import binascii
import os
import sys

from flask import redirect
from flask import request
from flask import render_template
from jia import app
from jia.auth import require_auth
from jia.decorators import json_endpoint
from jia.errors import PyCodeError
from jia.models import Board
from pykronos import KronosClient


@app.route('/', methods=['GET'])
@app.route('/<board_id>', methods=['GET'])
@require_auth
def index(board_id=None):
  if not board_id:
    # Create a new board and redirect to it.
    board = Board(id=binascii.b2a_hex(os.urandom(5)))
    board.save()
    return redirect('/%s' % board.id, code=302)
  board = Board.query.filter_by(id=board_id).first_or_404()
  return render_template('board.html', board=board)


@app.route('/board/<id>', methods=['GET', 'POST'])
@json_endpoint
@require_auth
def board(id=None):
  if request.method == 'POST':
    board = Board.query.filter_by(id=id).first_or_404()
    board.set_board_data(request.get_json())
    board.save()
  else:
    board = Board.query.filter_by(id=id).first_or_404()

  return board.json()


@app.route('/callsource', methods=['POST'])
@json_endpoint
@require_auth
def callsource(id=None):
  request_body = request.get_json()
  code = request_body.get('code')

  locals_dict = {
    'kronos_client': KronosClient(app.config['KRONOS_URL'],
                                  namespace=app.config['KRONOS_NAMESPACE']),
    'events': []
    }

  # TODO(marcua): We'll evenutally get rid of this security issue
  # (i.e., `exec` is bad!) once we build a query building interface.
  # In the meanwhile, we trust in user authentication.
  if code:
    try:
      exec code in {}, locals_dict # No globals.
    except:
      _, exception, tb = sys.exc_info()
      raise PyCodeError(exception, tb)

  events = sorted(locals_dict.get('events', []),
                  key=lambda event: event['@time'])
  response = {}
  response['events'] = events
  return response
