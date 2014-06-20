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
@require_auth
def index():
  return render_template('index.html')


@app.route('/<board_id>', methods=['GET'])
@require_auth
def redirect_old_board_url(board_id=None):
  """
  After switching to angular routing, board URLs changed.
  This redirect transfers old board URLs to the new ones and can probably be
  phased out eventually.
  """
  return redirect('/#/boards/%s' % board_id)


@app.route('/streams', methods=['GET'])
@json_endpoint
@require_auth
def streams():
  client = KronosClient(app.config['KRONOS_URL'],
                        namespace=app.config['KRONOS_NAMESPACE'])
  kronos_streams = client.get_streams(namespace=app.config['KRONOS_NAMESPACE'])
  kronos_streams = list(kronos_streams)
  return {
    'streams': kronos_streams,
  }


@app.route('/boards', methods=['GET'])
@json_endpoint
@require_auth
def boards(id=None):
  board_query = Board.query.all()
  boards = []
  for board in board_query:
    board_data = board.json()
    boards.append({
      'id': board_data['id'],
      'title': board_data['title'],
    })

  return {
    'boards': boards
  }


@app.route('/board/<id>', methods=['GET', 'POST'])
@json_endpoint
@require_auth
def board(id=None):
  if request.method == 'POST':
    board_data = request.get_json()
    if id == 'new':
      new_id = binascii.b2a_hex(os.urandom(5))
      board = Board(id=new_id)
      board_data['id'] = new_id
    else:
      board = Board.query.filter_by(id=id).first_or_404()
    board.set_board_data(board_data)
    board.save()
  else:
    board = Board.query.filter_by(id=id).first_or_404()

  return board.json()


@app.route('/board/<id>/delete', methods=['POST'])
@json_endpoint
@require_auth
def delete_board(id=None):
  board = Board.query.filter_by(id=id).first_or_404()
  board.delete()

  return {
    'status': 'success'
  }


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
