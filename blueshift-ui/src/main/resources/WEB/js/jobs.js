// Code goes here
var myapp = angular.module('mapp', []);
myapp.controller("mctrl", function($scope) {
    $scope.check = "";
    $scope.editmn = true;
    $scope.showfrm = false;
    $scope.edit = function() {
        $scope.editmode = true;
        $scope.rowUnderEdit = angular.copy($scope.currentrow);

    }
    $scope.cancel = function() {
        $scope.editmode = false;
        $scope.rowUnderEdit = {};
    }
    $scope.save = function() {
        $scope.currentrow.name = $scope.rowUnderEdit.name;
        $scope.currentrow.points = $scope.rowUnderEdit.points;
        $scope.editmode = false;
        $scope.resetAll({});
    }
    $scope.names = [{
        name: "John",
        points: 35
    }, {
        name: "David",
        points: 55
    }, {
        name: "Paul",
        points: 12
    },
        {
            name: "Allen",
            points: 23
        },

        {
            name: "Phill",
            points: 44
        }

    ];


    $scope.resetAll = function(currentRow) {
        angular.forEach($scope.names, function(val) {


            if (val.name !== currentRow.name) {
                val.checked = false;

            } else {
                $scope.currentrow = val;
            }
            $scope.editmn = false;
        });
    }




});