import binascii
import os

from flask import redirect
from flask import request
from flask import render_template
from jia import app
from jia.auth import require_auth
from jia.decorators import json_endpoint
from jia.models import Board
from jia.compute import QueryCompute, enable_precompute, disable_precompute
from jia.utils import get_seconds
from pykronos import KronosClient

@app.route('/status', methods=['GET'])
def status():
  """ A successful request endpoint without authentication.

  Useful for pointing load balancers/health checks at.
  """

  return "OK"

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

    old_panels = board.json()['panels']
    new_panels = board_data['panels']

    # Make panel dicts so they are searchable by ID
    try:
      old_panels = {p['id']: p for p in old_panels}
    except KeyError:
      # If the old panels do not have an id, then they are pre-precompute era.
      # Therefore none of them have precompute turned on, so for the purpose of
      # determining whether precompute has been enabled or changed, we can
      # pretend they don't exist.
      old_panels = {}
    new_panels = {p['id']: p for p in new_panels}

    # Find any changes to precompute settings
    for panel in old_panels.values():
      new_panel = new_panels.get(panel['id'])

      # Check for deletions
      if (panel['data_source']['precompute']['enabled'] and not new_panel):
        disable_precompute(panel)

      # Check for precompute disabled
      elif (panel['data_source']['precompute']['enabled']
            and new_panel
            and not new_panel['data_source']['precompute']['enabled']):
        disable_precompute(panel)

    for panel in new_panels.values():
      if panel['data_source']['precompute']['enabled']:
        old_panel = old_panels.get(panel['id'])

        # Check for precompute enabled
        if (not old_panel
            or not old_panel['data_source']['precompute']['enabled']):
          task_id = enable_precompute(panel)
          panel['data_source']['precompute']['task_id'] = task_id

        # Check for code change or precompute settings change
        elif (old_panel['data_source']['code'] != panel['data_source']['code']
              or old_panel['data_source']['precompute']
              != panel['data_source']['precompute']
              or old_panel['data_source']['timeframe']
              != panel['data_source']['timeframe']):
          disable_precompute(old_panel)
          task_id = enable_precompute(panel)
          panel['data_source']['precompute']['task_id'] = task_id

    # Transform panel dict back into list for saving
    new_panels = new_panels.values()

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
  precompute = request_body.get('precompute')
  timeframe = request_body.get('timeframe')
  bucket_width = get_seconds(precompute['bucket_width']['value'],
                             precompute['bucket_width']['scale'])

  task = QueryCompute(code, timeframe, bucket_width=bucket_width)
  events = task.compute(use_cache=precompute['enabled'])
  response = {}
  response['events'] = events
  return response
