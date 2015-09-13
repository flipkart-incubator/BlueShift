
var sourceConfig = function(){
    securityConfig = document.querySelector("#sourceSecurityConfig");
    connectionURL = document.querySelector("#sourceConnectionURL");
    user = document.querySelector("#sourceUser");
    password = document.querySelector("#sourcePassword");
    keyFile = document.querySelector("#sourceKeyFile");
    freeBytesSpace = document.querySelector("#sourceFreeBytes");
    destinationPath = document.querySelector("#sourceDestinationPath");
}

var sourceConfigVector = [  ]

var sourceApp = angular.module('source-insert-delete',[]);

sourceApp.controller('SourceIDCtrl',function($scope){
    $scope.sourceConfigs =  [{id:'sourceConfig1'}];

    $scope.addNewSourceConfig = function(){
        var newSourceConfig = $scope.sourceConfigs.length + 1;
        $scope.sourceConfigs.push({'id':'sourceConfig'+newSourceConfig})
        sourceConfigVector.push(new sourceConfig())
    };

    $scope.removeSourceConfig = function(){
        var lastSourceConfig = $scope.sourceConfigs.length - 1;
        $scope.sourceConfigs.splice(lastSourceConfig);
        sourceConfigVector.pop()
    };

    $scope.printSourceConfig = function(){
        return sourceConfigVector;
    }
});

var sinkApp = angular.module('sink-insert-delete',['source-insert-delete']);

sinkApp.controller("SinkIDCtrl",function($scope){
    $scope.sinkConfigs =  [{id:'sinkConfig1'}];

    $scope.addNewSinkConfig = function(){
        var newSinkConfig = $scope.sinkConfigs.length + 1;
        $scope.sinkConfigs.push({'id':'sinkConfig'+newSinkConfig})
    };

    $scope.removeSinkConfig = function(){
        var lastSinkConfig = $scope.sinkConfigs.length - 1;
        $scope.sinkConfigs.splice(lastSinkConfig);
    };

});

var toggleApp = angular.module('toggle-content',['sink-insert-delete']);

toggleApp.controller("ToggleCtrl",function ($scope) {
    $scope.toggle = false;

    $scope.getMsg = function(){
        if( $scope.toggle ){
            return 'Hide Options'
        }else{
            return 'More Options'
        }
    };
});



angular.module("config-root",['source-insert-delete','sink-insert-delete','toggle-content'])