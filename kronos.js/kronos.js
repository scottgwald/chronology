/*
 When running in a browser, ensure that the domain is added to 
 `node.cors_whitelist_domains` in settings.py.
*/

var KronosClient = function(kronosURL, namespace, debug) {
  // `kronosURL`: The URL of the Kronos server to talk to.
  // `namespace` (optional): Namespace to store events in.
  // `debug` (optional): Log error messages to console?
  var self = this;
  var putURL = kronosURL + '/1.0/events/put';
  var getURL = kronosURL + '/1.0/events/get';

  /**
   * Code from: https://gist.github.com/eriwen/2794392#file-cors-js
   *
   * Make a X-Domain request to url and callback.
   *
   * @param url {String}
   * @param method {String} HTTP verb ('GET', 'POST', 'DELETE', etc.)
   * @param data {String} request body
   * @param callback {Function} to callback on completion
   * @param errback {Function} to callback on error
   */
  var xdr = function(url, method, data, callback, errback) {
    var req;
    
    if(XMLHttpRequest) {
      req = new XMLHttpRequest();
 
      if('withCredentials' in req) {
        req.open(method, url, true);
        req.onerror = errback;
        req.onreadystatechange = function() {
          if (req.readyState === 4) {
            if (req.status >= 200 && req.status < 400) {
              if (!callback) return;
              callback(req.responseText);
            } else {
              if (!errback) return;
              errback(new Error('Response returned with non-OK status.'));
            }
          }
        };
        req.send(data);
      }
    } else if(XDomainRequest) {
      req = new XDomainRequest();
      req.open(method, url);
      req.onerror = errback;
      req.onload = function() {
        if (!callback) return;
        callback(req.responseText);
      };
      req.send(data);
    } else {
      if (!errback) return;
      errback(new Error('CORS not supported.'));
    }
  }

  this.url = kronosURL;
  this.namespace = namespace || null;

  this.put = function(stream, event, namespace) {
    // `stream`: Stream to put the event in.
    // `event`: The event object.
    // `namespace` (optional): Namespace for the stream.
    event = event || {};
    if (event['@time'] == null) {
      event['@time'] = (new Date()).getTime() * 1e4;
    }

    var data = {namespace: namespace || self.namespace, events: {}};
    data.events[stream] = [event];
    data = JSON.stringify(data);
    
    xdr(putURL,
        'POST',
        data,
        function(data) {
          if (!debug) return;
          data = JSON.parse(data);
          // Check if there was a server side error?
          if (data['@errors'] || data[stream] != 1) {
            console.log('KronosClient.put encountered a server error: ' +
                        data['@errors'] || 'unknown' + '.');
          }
        },
        function(errorThrown) {
          // TODO(usmanm): Add retry logic?
          if (!debug) return;
          console.log('KronosClient.put request failed with error: ' +
                      errorThrown + '.');
        });
  }
};
