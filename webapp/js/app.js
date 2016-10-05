var latestViolationsUrl = "https://api.evanstoninspector.com/";

var violationsStore = {
  state: {
    violations: [],
  },
  fetchRecent: function(callback) {
    var self = this;
    $.ajax(latestViolationsUrl, {
      // need to do this to force CORS preflight OPTIONS request
      contentType: "application/json"
    }).done(function (data) {
      self.state.violations = data;
      callback();
    });
  },

  fetchSingleBusinessViolations: function(businessId, callback) {
    var self = this;
    $.ajax(latestViolationsUrl + 'businesses/' + businessId, {
      // need to do this to force CORS preflight OPTIONS request
      contentType: "application/json"
    }).done(function (data) {
      self.state.violations = data;
      callback();
    });
  }
}

Handlebars.registerHelper('friendlyDate', function(date) {
  return parseInt(date.substring(4, 6)) + '/' + parseInt(date.substring(6, 8)) + '/' + date.substring(0, 4);
});


function displayHome(router, templates) {
  var context = { title: "Latest Violations", body: '<img src="/img/default.gif">' };
  var html = templates.home(context);
  $('#app').html(html);
  router.updatePageLinks();
  violationsStore.fetchRecent(function() {
    context.body = templates.violationList({violations: violationsStore.state.violations});
    html = templates.home(context);
    $('#app').html(html);
    router.updatePageLinks();
  });
}

function displayBusiness(router, templates, id) {
  var context = { title: id, body: '<img src="/img/default.gif">' };
  var html = templates.business(context);
  $('#app').html(html);
  router.updatePageLinks();
  violationsStore.fetchSingleBusinessViolations(id, function() {
    context.body = templates.violationList({violations: violationsStore.state.violations});
    html = templates.business(context);
    $('#app').html(html);
    router.updatePageLinks();
  })
}

$(document).ready(function() {
  var templates = {};
  templates.home = Handlebars.compile($("#home-template").html());
  templates.business = Handlebars.compile($("#business-template").html());
  templates.violationList = Handlebars.compile($("#violations-list-template").html());
  
  var router = new Navigo();
  router
  .on(function () {
    displayHome(router, templates);
  })
  .on('/business/:id/', function (params) {
    displayBusiness(router, templates, params.id);
  })
  .resolve();
});
