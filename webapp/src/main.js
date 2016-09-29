var latestViolationsUrl = "https://api.evanstoninspector.com/";

var violationsStore = {
  state: {
    violations: [],
  },
  fetch: function() {
    var self = this;
    $.ajax(latestViolationsUrl, {
      // need to do this to force CORS preflight OPTIONS request
      contentType: "application/json"
    }).done(function (data) {
      self.state.violations = data;
    });
  }
}

var app = new Vue({
  el: '#app',
  data: violationsStore.state,
});

violationsStore.fetch();