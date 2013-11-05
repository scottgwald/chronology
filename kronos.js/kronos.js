/*
 When running in a browser, ensure that the domain is added to 
 `node.cors_whitelist_domains` in settings.py.
*/

var KronosClient = function(kronosURL, namespace, jQuery, debug) {
  // `kronosURL`: The URL of the Kronos server to talk to.
  // `jQuery`: The jQuery ($) object.
  // `namespace` (optional): Namespace to store events in.
  // `debug` (optional): Log error messages to console?
  var self = this;
  var putURL = kronosURL + '/1.0/events/put';
  var getURL = kronosURL + '/1.0/events/get';

  if (!jQuery) {
    throw new Error('jQuery argument must not be null.');
  }

  var $ = jQuery;

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
    
    $.ajax({
      url: putURL,
      type: 'POST',
      data: JSON.stringify({stream: [event],
                            namespace: namespace || self.namespace}),
      crossDomain: true,
      processData: false,
      dataType: 'json',
      success: function(data, textStatus, jqXHR) {
        if (!debug) return;
        // Check if there was a server side error?
        if (data['@errors'] || data[stream] != 1) {
          console.log('KronosClient.put encountered a server error: ' +
                      data['@errors'] || 'unknown' + '.');
        }
      },
      error: function(jqXHR, textStatus, errorThrown) {
        // TODO(usmanm): Add retry logic?
        if (!debug) return;
        console.log('KronosClient.put request failed with status code: ' +
                    textStatus + '. ' + errorThrown);
      },
    });
  }
};
