//
//var sourceConfig = function(){
//    securityConfig = document.querySelector("#sourceSecurityConfig");
//    connectionURL = document.querySelector("#sourceConnectionURL");
//    user = document.querySelector("#sourceUser");
//    password = document.querySelector("#sourcePassword");
//    keyFile = document.querySelector("#sourceKeyFile");
//    freeBytesSpace = document.querySelector("#sourceFreeBytes");
//    destinationPath = document.querySelector("#sourceDestinationPath");
//}
var sourceConfigVector = [  ]

angular.module('blueshift-rest-config',[])
    .controller('BlueshiftCtrl',function($scope){
        $scope.blueshiftConfig = {},
        $scope.blueshiftConfig.dcmConfig = {},
        $scope.blueshiftConfig.dcmConfig.sourceConfig = {},
        $scope.blueshiftConfig.dcmConfig.sourceConfig.connectionConfig = [];
    });


var sourceApp = angular.module('source-insert-delete',['blueshift-rest-config']);

sourceApp.controller('SourceIDCtrl',function($scope){

    var connectionConfig = {
        'securityConfig':'',
        'connectionURL':'',
        'user':'',
        'password':'',
        'keyFile':''
    };
    $scope.configValues = [];
    $scope.addNewConfig = function(){
        $scope.configValues.push(angular.copy(connectionConfig));
        $scope.blueshiftConfig.dcmConfig.sourceConfig.connectionConfig = $scope.configValues;
    }

    $scope.removeConfig = function(){
        if( ($scope.configValues.length - 1) > 0 )
            $scope.configValues.splice($scope.configValues.length - 1);
            $scope.blueshiftConfig.dcmConfig.sourceConfig.connectionConfig = $scope.configValues;
    }

    var _initialize = function(){
        $scope.addNewConfig();
    }

    _initialize();

});

var sinkApp = angular.module('sink-insert-delete',['source-insert-delete']);

sinkApp.controller("SinkIDCtrl",function($scope){
    var connectionConfig = {
        'securityConfig':'',
        'connectionURL':'',
        'user':'',
        'password':'',
        'keyFile':'',
        'freeBytesSpace':'0',
        'destinationPath':''
    };
    $scope.configValues = [];
    $scope.addNewConfig = function(){
        $scope.configValues.push(angular.copy(connectionConfig));
        $scope.blueshiftConfig.dcmConfig.sinkConfig.connectionConfig = $scope.configValues;
    }

    $scope.removeConfig = function(){
        if( ($scope.configValues.length - 1) > 0 )
            $scope.configValues.splice($scope.configValues.length - 1);
            $scope.blueshiftConfig.dcmConfig.sinkConfig.connectionConfig = $scope.configValues;
    }

    var _initialize = function(){
        $scope.addNewConfig();
    }

    _initialize();

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
    .controller('RootCtrl',function($scope,$http){
        $scope.blueshiftConfig = {};
        $scope.blueshiftConfig.transferGroupName = '';
        $scope.blueshiftConfig.transferJobName  = '';
        $scope.blueshiftConfig.cronScheduleStr  = '';
        $scope.blueshiftConfig.dcmConfig = {};
        $scope.blueshiftConfig.dcmConfig.batchName  = '';
        $scope.blueshiftConfig.dcmConfig.numWorkers  = 0;
        $scope.blueshiftConfig.dcmConfig.ignoreException  = false;
        $scope.blueshiftConfig.dcmConfig.localModeExecution = false;
        $scope.blueshiftConfig.dcmConfig.maxRetries = 0;
        $scope.blueshiftConfig.dcmConfig.statusPath = '';
        $scope.blueshiftConfig.dcmConfig.stateManagerType = '';
        $scope.blueshiftConfig.dcmConfig.sourceConfig = {};
        $scope.blueshiftConfig.dcmConfig.sourceConfig.path  = '';
        $scope.blueshiftConfig.dcmConfig.sourceConfig.includeListFile = '';
        $scope.blueshiftConfig.dcmConfig.sourceConfig.excludeListFile = '';
        $scope.blueshiftConfig.dcmConfig.sourceConfig.includeRegEx = '';
        $scope.blueshiftConfig.dcmConfig.sourceConfig.excludeRegEx = '';
        $scope.blueshiftConfig.dcmConfig.sourceConfig.startTS = '';
        $scope.blueshiftConfig.dcmConfig.sourceConfig.endTS  = '';
        $scope.blueshiftConfig.dcmConfig.sourceConfig.minFilesize = '';
        $scope.blueshiftConfig.dcmConfig.sourceConfig.maxFilesize  = '';
        $scope.blueshiftConfig.dcmConfig.sourceConfig.deleteSource = false;
        $scope.blueshiftConfig.dcmConfig.sourceConfig.ignoreEmptyFiles  = true;
        $scope.blueshiftConfig.dcmConfig.sourceConfig.transformSource = false;
        $scope.blueshiftConfig.dcmConfig.sourceConfig.includeUpdatedFiles =  false;
        $scope.blueshiftConfig.dcmConfig.sourceConfig.compressionThreshold = 0;
        $scope.blueshiftConfig.dcmConfig.sourceConfig.customSourceImplClass = '';
        $scope.blueshiftConfig.dcmConfig.sourceConfig.connectionConfig = [];
        $scope.blueshiftConfig.dcmConfig.sinkConfig = {};
        $scope.blueshiftConfig.dcmConfig.sinkConfig.path  =  "";
        $scope.blueshiftConfig.dcmConfig.sinkConfig.compressionCodec  =  "none";
        $scope.blueshiftConfig.dcmConfig.sinkConfig.useCompression = false;
        $scope.blueshiftConfig.dcmConfig.sinkConfig.overwriteFiles = true;
        $scope.blueshiftConfig.dcmConfig.sinkConfig.append =  false;
        $scope.blueshiftConfig.dcmConfig.sinkConfig.customSinkImplClass = "";
        $scope.blueshiftConfig.dcmConfig.sinkConfig.connectionConfig = [];
        $scope.blueshiftConfig.dcmConfig.dbConfig = {};
        $scope.blueshiftConfig.dcmConfig.dbConfig.dbConnectionURL = "",
        $scope.blueshiftConfig.dcmConfig.dbConfig.dbDriver =  "";
        $scope.blueshiftConfig.dcmConfig.dbConfig.dbUserName = "";
        $scope.blueshiftConfig.dcmConfig.dbConfig.dbUserPassword = "";
        $scope.blueshiftConfig.dcmConfig.dbConfig.dbDialect = "";


        $scope.submitJob = function(){
            console.log(JSON.stringify($scope.blueshiftConfig));
            $http.post("/blueshift/scheduler/transfer",JSON.stringify($scope.blueshiftConfig));
            alert($scope.blueshiftConfig.transferGroupName+" : "+$scope.blueshiftConfig.transferGroupName.transferJobName+" transfer initiated!!!\n")

        }

        // function to submit the form after all validation has occurred
        $scope.triggerTransfer = function() {
            $http.post("/blueshift/scheduler/transfer/once")

        };
    });


$("button").on("click", function () {
    var state = $(this).data('state');
    state = !state;
    if (state) {
        $("span").addClass("show");
    } else {
        $("span").removeClass("show");
    }
    $(this).data('state', state);
});

//// create angular app
//var validationApp = angular.module('validationApp', []);
//
//// create angular controller
//validationApp.controller('mainController', function($scope) {
//
//    // function to submit the form after all validation has occurred
//    $scope.submitForm = function() {
//
//        // check to make sure the form is completely valid
//        if ($scope.configForm.$valid) {
//            alert('our form is amazing');
//        }
//
//    };
//
//});