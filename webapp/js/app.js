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

function displayLoading(router, templates) {
  $('#app').html(templates.loading({}));
  router.updatePageLinks();
}

function setupSearch() {
  var awesomplete = new Awesomplete(document.getElementById("search"));
  $('#search').on('keyup', function(evt) {
    if (evt.which == 40) {
      // down arrow
      return;
    }
    var searchString = $(evt.target).val();
    if (searchString.length < 3) {
      return;
    }
    $.ajax(latestViolationsUrl + 'search?s=' + searchString, {
      // need to do this to force CORS preflight OPTIONS request
      contentType: "application/json"
    })
    .done(function (data) {
      var list = [];
      for (var key in data) {
        list.push({label: key, value: data[key]})
      }
      awesomplete.list = list;
      });
    });
}

function displayHome(router, templates) {
  var context = { title: "Evanston Inspection Violations", body: '<img src="/img/default.gif">' };
  var html = templates.home(context);
  $('#app').html(html);
  setupSearch();
  router.updatePageLinks();
  violationsStore.fetchRecent(function() {
    context.body = templates.violationList({violations: violationsStore.state.violations});
    html = templates.home(context);
    $('#app').html(html);
    setupSearch();
    router.updatePageLinks();
  });
}

function displayBusiness(router, templates, id) {
  displayLoading(router, templates);
  violationsStore.fetchSingleBusinessViolations(id, function() {
    var context = {
      name: violationsStore.state.violations[0].name,
      address: violationsStore.state.violations[0].address,
      violations: violationsStore.state.violations
    };   
    html = templates.business(context);
    $('#app').html(html);
    router.updatePageLinks();
  })
}

$(document).ready(function() {
  var templates = {};
  templates.home = Handlebars.compile($("#home-template").html());
  templates.loading = Handlebars.compile($("#loading-template").html());
  templates.business = Handlebars.compile($("#business-template").html());
  templates.violationList = Handlebars.compile($("#violations-list-template").html());
  
  var router = new Navigo();

  window.addEventListener("awesomplete-selectcomplete", function(e) {
    router.navigate('/business/' + $('#search').val() + '/');
  }, false);

  router
  .on(function () {
    displayHome(router, templates);
  })
  .on('/business/:id/', function (params) {
    displayBusiness(router, templates, params.id);
  })
  .resolve();
});
