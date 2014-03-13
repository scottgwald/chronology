// `getTime` returns the `Date` object in number of milliseconds since epoch.
// Add function to covert that into 100ns resolution.
Date.prototype.toKronosTime = function() {
  return this.getTime() * 10000;
};

Date.prototype.toSeconds = function() {
  return this.getTime() / 1000.0;
};

Date.fromKronosTime = function(kronosTime) {
  return new Date(kronosTime / 10000.0);
};