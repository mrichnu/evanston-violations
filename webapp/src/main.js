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

Vue.component('violation-item', {
  props: ['violation'],
  template: '<li>{{ datestr }} {{ violation.name }}: <span>{{ violation.description }}</span></li>',
  computed: {
    datestr: function() {
      var y = this.violation.date.substring(0, 4);
      var m = this.violation.date.substring(4, 6);
      var d = this.violation.date.substring(6, 8);
      return `${m}/${d}/${y}`;
    },
  },
});

var app = new Vue({
  el: '#app',
  data: violationsStore.state,
});

violationsStore.fetch();