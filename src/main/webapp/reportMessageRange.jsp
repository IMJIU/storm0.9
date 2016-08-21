<%@ page language="java" pageEncoding="UTF-8" %>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>
    <%@include file="/WEB-INF/pages/admin/resource.jsp" %>
</head>
<body class="easyui-layout">
<div class="ui-search-panel" region="north" style="height: 80px;" title="选择条件"
     data-options="striped: false,collapsible:false,iconCls:'icon-search',border:false">
    <p class="ui-fields">
        <label class="ui-label">起始日期</label>
        <input id="startDate" class="easyui-datebox" >
        <label class="ui-label">结束日期</label>
        <input id="endDate" class="easyui-datebox">
        <a id="btn-search" class="easyui-linkbutton" iconCls="icon-search" onclick="searchData()">搜索</a>
    </p>
</div>
<!-- Search panel start -->
<div id="p" class="ui-search-panel" region="center"
     style="padding:10px;background:#fafafa;"
     data-options="iconCls:'icon-save',closable:true, collapsible:true,minimizable:true,maximizable:true">
    <div id="main" style="height:600px;border:1px solid #ccc;padding:10px;"></div>
</div>
<script src="/js/echarts.min.3.1.9.js"></script>
<script type="text/javascript" src="/js/admin/sys/reportMessageRange.js"></script>
</body>
</html>
