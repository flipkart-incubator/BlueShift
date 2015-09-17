var app = angular.module("app", ["xeditable"]);

app.run(function(editableOptions) {
    editableOptions.theme = 'bs3';
});

app.controller('Ctrl', function($scope, $filter, $http) {
    $http.get('/blueshift/scheduler/status').success(function (data) {
        $scope.rows = data;
    });


    $scope.checkField = function (data) {
        if (data == null || data == '') {
            return "Cannot be empty";
        }
    };

    $scope.saveUser = function (data) {
        var path = '/blueshift/scheduler/transfer/update/cron'
        var parameter = JSON.stringify(data);
        console.log(parameter);
        return $http.post(path, parameter);
    };

    // remove user
    $scope.deleteRow = function (index) {
        var path = '/blueshift/scheduler/remove/' + $scope.rows[index].namespace + '/' + $scope.rows[index].jobName;
        $scope.rows.splice(index, 1);
        $http.get(path);

    };
});
