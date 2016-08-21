var myChart = null;
$(function(){
    var timeData = ["00:00","00:20","00:40","01:00","01:20","01:40","02:00","02:20","02:40","03:00","03:20","03:40","04:00","04:20","04:40","05:00","05:20","05:40","06:00","06:20","06:40","07:00","07:20","07:40","08:00","08:20","08:40","09:00","09:20","09:40","10:00","10:20","10:40","11:00","11:20","11:40","12:00","12:20","12:40","13:00","13:20","13:40","14:00","14:20","14:40","15:00","15:20","15:40","16:00","16:20","16:40","17:00","17:20","17:40","18:00","18:20","18:40","19:00","19:20","19:40","20:00","20:20","20:40","21:00","21:20","21:40","22:00","22:20","22:40","23:00","23:20","23:40"];
    option = {
        title: {
            text: '患者医生消息发送频率',
            subtext: '冬日暖阳',
            x: 'center'
        },
        tooltip: {
            trigger: 'axis',
            formatter: function (params) {
                var msg = "";
                for(var i=0;i<params.length;i++){
                    msg += params[i].name + ' '+ params[i].seriesName + ' : ' + params[i].value + ' (条)<br/>'
                }
                return msg;
            },
            axisPointer: {
                animation: false
            }
        },
        legend: {
            data: [],
            x: 'left'
        },
        dataZoom: [
            {
                show: true,
                realtime: true,
                start: 30,
                end: 100,
                xAxisIndex: [0, 1]
            },
            {
                type: 'inside',
                realtime: true,
                start: 30,
                end: 100,
                xAxisIndex: [0, 1]
            }
        ],
        grid: [{
            left: 50,
            right: 50,
            height: '35%'
        }, {
            left: 50,
            right: 50,
            top: '55%',
            height: '35%'
        }],
        xAxis: [
            {
                type: 'category',
                boundaryGap: false,
                axisLine: {onZero: true},
                data: timeData
            },
            {
                gridIndex: 1,
                type: 'category',
                boundaryGap: false,
                axisLine: {onZero: true},
                data: timeData,
                position: 'top'
            }
        ],
        yAxis: [
            {
                name: '患者消息(条)',
                type: 'value'
            },
            {
                gridIndex: 1,
                name: '医生消息(条)',
                type: 'value',
                inverse: true
            }
        ],
        series: []
    };
    myChart = echarts.init(document.getElementById('main'));
    var now = new Date().Format("yyyyMMdd")
    requestData(now,now);
})


    /**
     * 请求数据
     */
    function requestData(startDate,endDate){
        var param = {};
        if(startDate)
          param.startDate = startDate;
        if(endDate)
          param.endDate = endDate;

        $.getJSON("getData",param,  function(data){
            var lengend = [];
            var series = [];

            for(var key in data.data){
                var userType = key.charAt(9);
                var lineName = key.replace('2016','').replace("-1",'-患者').replace("-2","-医生");
                var lineData = data.data[key];
                lengend.push(lineName);
                var s = {};
                s.name = lineName;
                s.type='line';
                s.symbolSize=8;
                s.hoverAnimation=false;
                if(userType=='2'){
                    s.xAxisIndex=1;
                    s.yAxisIndex=1;
                }
                s.data = lineData;
                series.push(s);
            }
            option.legend.data=lengend;
            option.series=series;
            if(myChart){
                myChart.clear();
            }
            myChart.setOption(option);
        });
    }
/**
 * 搜索数据
 */
function searchData() {

    var startDate = $('#startDate').datebox('getValue');
    var endDate = $('#endDate').datebox('getValue');
    var d_start = new Date();
    var d_end = new Date();

    //todo 时间范围控制
    if(startDate){
        d_start.setFullYear(startDate.substr(0,4),parseInt(startDate.substr(5,2))-1,startDate.substr(8,2))
        startDate = d_start.Format("yyyyMMdd")
    }
    if(endDate){
        d_end.setFullYear(endDate.substr(0,4),parseInt(endDate.substr(5,2))-1,endDate.substr(8,2))
        endDate = d_end.Format("yyyyMMdd")
    }
    requestData( startDate.replace(/-/g,''),endDate.replace(/-/g,''));
}