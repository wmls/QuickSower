function stopBubble(e) {
    var evt = e ? e : event;
    if (evt && evt.stopPropagation)
        evt.stopPropagation();
    else
        evt.cancelBubble = true;
}
function getContainer(oe, jattr, firstStop) {
    //jattr:{tagname,attributename,attrbutevalue} ,tagname,attributename两参数表示container的查找属性，可以不必全定义
    //tagname:表示目标Container的tagname，可选参数，定义此参数，则循环到此元素，无论属性值是否满足条件都终止循环
    var o;
    var b = firstStop == undefined ? true : firstStop;  //firstStop：true表示遇到第一个tagname符合的就终止，tagname没定义就看属性名称是否符合
    var tag = jattr.tagName, atname = jattr.attrName, atvalue = jattr.attrValue;
    while (oe = oe.parentNode) {
        var otag = oe.tagName;
        if (tag != undefined) {  //按container的tagname属性定位
            if (otag.toLowerCase() == tag.toLowerCase()) {
                if (atname != undefined) {
                    if (oe.hasAttribute(atname)) {
                        if (atvalue != undefined) {
                            if (oe.getAttribute(atname).toLowerCase() == atvalue.toLowerCase()) {
                                o = oe; break;
                            }
                        }
                        if (b) { o = oe; break; } //如果同时定义了tag和属性，则属性优先
                    }
                } else {
                    if (b) { o = oe; break; }
                }
            }
        } else {
            if (atname != undefined && oe.hasAttribute(atname)) {
                if (atvalue != undefined) {
                    if (oe.getAttribute(atname).toLowerCase() == atvalue.toLowerCase()) {
                        o = oe; break;
                    }
                }
                if (b) { o = oe; break; }
            }
        }
    }
    return o;
}
Array.prototype.indexOf = function (e) {
    for (var i = 0, j; j = this[i]; i++) {
        if (j == e) { return i; }
    }
    return -1;
}
Array.prototype.lastIndexOf = function (e) {
    for (var i = this.length - 1, j; j = this[i]; i--) {
        if (j == e) { return i; }
    }
    return -1;
}
if (!String.prototype.trim) { var TRIM_REG = /^\s+|\s+$/g; String.prototype.trim = function () { return this.replace(TRIM_REG, ''); } }
if (!String.prototype.isNumber) { String.prototype.isNumber = function () { return (new RegExp(/^\d+$/)).test(this); } }
var isArray = function (obj) {
    return Object.prototype.toString.call(obj) === '[object Array]';
}
var isJson = function (obj) {
    var isjson = typeof (obj) == "object" && Object.prototype.toString.call(obj).toLowerCase() == "[object object]" && !obj.length;
    return isjson;
}
function getUrlpara(paname) {
    var u = window.location.search;
    var ar = u.replace(/\?/, '').split(/&/);
    var rtn;
    for (var i = 0; i < ar.length; i++) { if (ar[i].split('=')[0].indexOf(paname) >= 0) { rtn = ar[i].substr(ar[i].indexOf('=') + 1); break; } }
    return rtn;
}
function setpara(u, ar) {
    for (var i = 0; i < ar.length; i++) {
        u = u.replace('[' + (i + 1) + ']', ar[i]);
    }
    return u;
}
var mAttr = function (ims) { for (var m = 0; m < arMS.length; m++) { if (arMS[m][0] == ims) return arMS[m][1]; return ''; } };
var ar_Section = new Array(), ar_CountModel = new Array();
var finishIniStruct = false;
var frameChanged = (dotype == "new" || dotype == "add" ? true : false);
var isFirstLoadMenuTree = true;
var viewbkgcolor = '#eeeeee'; //数据区背景颜色
var highlightcolor = '#f1f1f1';  //数据区高亮背景色cccccc
var viewIndent = true;  //数据区是否缩进
var viewstyle = '01';   //排版类型：01:平铺，02:Tab
var structline = true;  //是否显示结构线
var noLockObj = null;
var specialNR = '';
var specialNodes = '';
$(function () {
    //异步处理要注意防止ajax服务端的代码重入问题，会造成参数错误，或由于不同交互事务的代码片段的执行相互干扰导致一些事务的意外截断或中止
    $('#menu_nr').tree({
        url: url_nr,
        animate: true,
        checkbox: false,
        lines: true,
        onContextMenu: onContextMenu,
        formatter: function (node) {
            return setnode(node);
        },
        onClick: function (node) {
            //setDispData("edit", true);
            var osel = $('div[currentNode]');
            if (osel.length > 0) {
                restorebg(osel);
                function restorebg(os) {
                    restdiv(os);
                    $("div[id^='p_']", os).each(function () {
                        restdiv(this);
                        if ($("div[id^='p_']", this).length > 0) restorebg(this);
                    });
                }
                function restdiv(odiv) {
                    var iscontainer = $(odiv).attr('container') ? true : false;
                    $(odiv).css({ 'border-width': (iscontainer ? (viewIndent ? '1' : '0') + 'px' : 'none'), 'border-color': 'lightgray', 'background': (iscontainer ? 'white' : viewbkgcolor) });
                }
                osel.removeAttr('currentNode');
            }
            $("div[id^='p_']", $('#p_' + node.id)).each(function () {
                $(this).attr('currentNode', true).css({ 'background': viewbkgcolor });
            });
            $('#p_' + node.id).attr('currentNode', true).css({ 'border-width': '1px', 'border-style': 'solid', 'border-color': 'red', 'background': highlightcolor });
        },
        onExpand: function (node) {
            expand(node);
        },
        onCollapse: function (node) {
            collapse(node);
        },
        onBeforeLoad: function (node, param) {
            //setDataControl();
        },
        onLoadError: function (arguments) {
            alert(arguments.responseText);
        },
        onLoadSuccess: function (node, data) {
            if (data && eval(data).error) {
                alert(eval(data).error);
            } else {
                frameChanged = finishIniStruct ? true : (dotype == "new" || dotype == "add" ? true : false);
                if (!finishIniStruct) finishIniStruct = true;

                addChanges('add', (Object.prototype.toString.call(data) == '[object Array]' ? data : [data]));
                var curnode = tmpR.eNode;
                if (curnode) {
                    //处理被依赖节点的子节点（包括在事务中新添加的新节点）的依赖事务
                    var ritems = tmpR.relies;
                    for (var ii = 0; ii < ritems.length; ii++) {
                        var otmpa = ritems[ii].attributes;
                        var b = insertRelyOptions(curnode, otmpa.dataid, otmpa.keyname, otmpa.keypath, transbool(otmpa.ismultiple), otmpa.dataparent);
                        tmpR.lastIndex = (b && tmpR.lastIndex < ii ? ii : tmpR.lastIndex);
                    }
                    if (ritems.length > 0 && data[0].attributes.dataid == ritems[tmpR.lastIndex].attributes.dataid) {
                        disLoadmasklayer();
                        //初始化依赖项管理器
                        tmpR.eNode = null;
                        tmpR.relies = new Array();
                        tmpR.lastIndex = 0;
                    }
                }
                if ($("#loading").length > 0) {
                    show();
                } else {
                    setupSection();
                }
            }
            if (isFirstLoadMenuTree && (dotype != "new" && dotype != "add")) {
                resetChanges();
                
            }
        }
    });
});
function getSpecialNodes(nrcode) {
    for (var i = 0; i < specialNonLock.length; i++) {
        var olock = specialNonLock[i];
        if (olock.NR.toLowerCase() == nrcode.toLowerCase()) return olock;
    }
    return null;
}
function setDispData(type, chk) {
    var sel = $('#menu_nr').tree('getSelected');
    var t = $('#menu_nr').tree('getRoot');
    var ctnobjs = getLastNode(type, chk, { ar: new Array(), lastnode: t });
    var lastn = ctnobjs.lastnode ? ctnobjs.lastnode : t;
    var topn = ctnobjs.topnode ? ctnobjs.topnode : t;
    var arctn = ctnobjs.ar;
    var isactiveE = false; //是否激发事件
    var fun = function (p) {
        if (type == "edit" && chk) {
            b(sel);
            setTimeout(function () { $(sel).click; }, 500);
        }
        isactiveE = true;
        disLoadmasklayer();
    };
    for (var i = 0; i < arctn.length; i++) {
        if (arctn[i]) {
            var op = $('#p_' + arctn[i].id);
            var doattr = arctn[i].doattr;
            var isdo = doattr.isdo;  //是否会引发expand或collapse事件
            var dotype = doattr.dotype;
            if (op && i + 1 < arctn.length && isdo) {
                if (dotype == 'expand') {
                    op.panel('options').onExpand = function () {
                        setTimeout(function () { _a(type == "check" ? chk : !chk); });
                    };
                } else {
                    op.panel('options').onCollapse = function () {
                        setTimeout(function () { _a(type == "check" ? chk : !chk); });
                    };
                }
            }
        }
    }
    if (arctn.length > 0) loadmasklayer("正在整理数据,请稍候......");
    setTimeout(function () { _a(0, (type == "check" ? chk : !chk)); });

    function _a(isshow) {
        while (arctn.length > 0) {
            var a = arctn[0]; t = a.doattr;
            var id = a.id;
            var dotype = t.dotype;
            var isdo = t.isdo;
            var p = $('#p_' + id);
            arctn.splice(0, 1);
            if (isdo) {
                p_show(p, dotype);
                break;
            }
            if (id == topn.id) fun(p);
        }
    }
    function b(node) {
        if (node) {
            var k = node.attributes.keyname;
            var p = $('#p_' + node.id);
            if (p.length > 0 && p.hasClass("easyui-panel") && p.panel('header').length > 0) {
                $(p).panel('expand', true);
            }
            node = $('#menu_nr').tree('getParent', node.target);
            if (node) b(node);
        }
    }
    function p_show(p, dotype) {
        if (dotype) {
            $(p).panel(dotype, 'animate');
            if (p[0].id == 'p_' + topn.id) {
                if (!isactiveE) {  //一般是初始状态与当前参数一致导致没激发expand或collapse事件
                    disLoadmasklayer();
                }
            }
        }
    }
}
function getLastNode(type, isChecked, nobj) {//返回当前节点的所有嵌套子集的最后一个节点
    var ar = nobj.ar;
    var n = nobj.lastnode;
    var chd = n.children;
    var pn; pn = _j(n) ? n : pn;
    var findlast = false;
    var lastn = pn;
    var topn = nobj.topnode;
    var tmpn = n;
    if (chd) {
        for (var i = chd.length - 1; i >= 0; i--) {
            pn = _j(chd[i]) ? chd[i] : pn;
            var _chd = chd[i].children;
            if (_chd) {
                if (!topn && (pn.attributes.keyname == 'deal' || pn.attributes.keyname == 'vr')) topn = pn;
                if (!lastn || lastn.id < pn.id) lastn = pn;
                var o = getLastNode(type, isChecked, { ar: ar, lastnode: chd[i] });
                tmpn = o.lastnode;
                if (lastn.id < tmpn.id) lastn = tmpn;
                ar = o.ar;
                if ($('#p_' + chd[i].id).length > 0) addEle(ar, { id: (pn ? pn.id : null), doattr: a(chd[i], type, isChecked) });
            }
        }
        if ($('#p_' + n.id).length > 0) addEle(ar, { id: (n ? n.id : null), doattr: a(n, type, isChecked) });
    }
    function a(node, type, isChecked) {
        var ctn = node.id, leaf, dotype;
        var op = $('#p_' + node.id);
        ctn = $(op).attr('hasContainer');
        var bctn = (ctn && ctn == 'true' ? true : false);
        leaf = $(op).attr('hasLeaf');
        var bleaf = (leaf && leaf == 'true' ? true : false);
        var ischanged = (node.attributes.state == "1" ? true : false);
        if (type == "check") {
            dotype = (isChecked && !ischanged ? (bctn && !bleaf ? 'expand' : 'collapse') : 'expand');
        } else {
            dotype = (!isChecked ? 'expand' : (bctn && !bleaf ? 'expand' : 'collapse'));
        }
        var isdo = ($(op).panel('options').collapsed ? (dotype == 'expand' ? true : false) : (dotype == 'expand' ? false : true));
        return { dotype: dotype, isdo: isdo };
    }
    function addEle(ar, ele) {
        var isexist = false;
        for (var i = 0; i < ar.length; i++) {
            if (ar[i].id == ele.id) {
                isexist = true;
                break;
            }
        }
        if (!isexist) ar.push(ele);
    }
    function _j(p) {
        return (p && $('#p_' + p.id) && $('#p_' + p.id).hasClass("easyui-panel") ? p : null);
    }
    return { ar: ar, topnode: topn, lastnode: lastn };
}

function setnode(node, isunitpolicy) {
    function flowctrl(ctrl) {
        if (typeof ctrl == 'boolean') return ctrl;
        var b = false;
        if (ctrl == undefined || ctrl == null) {
            return b;
        } else {
            if (ctrl == '*' || ctrl.indexOf('*') > -1) {
                return true;
            }
        }
        var pkey = ctrl.split(',');
        for (var i = 0; i < pkey.length; i++) {
            if (('-'+node.attributes.keypath+'-').toLowerCase().indexOf(('-'+pkey[i]+'-').toLowerCase()) > -1) { b = true; break; }
        }
        return b;
    }
    if (node) {
        var na = node.attributes, id = node.id;
        var isoptional = (na.optional.toLowerCase() == "true" ? true : false);
        var ismulti = (na.ismultiple.toLowerCase() == "true" ? true : false);
        var isrely = (na.relyonnode != "" ? true : false);
        var isnoflowctrl = flowctrl(na.noflowcontrol);
        na.noflowcontrol = isnoflowctrl;

        var isspecialnolock = false;  //是否被指定为解锁
        if (noLockObj != null) {
            if (specialNodes == null || specialNodes == '') {
                isspecialnolock = true;
            } else {
                if ((',' + specialNodes + ',').indexOf(',' + na.nr005id + ',') >= 0) {
                    isspecialnolock = true;
                } else {
                    isspecialnolock = false;
                }
            }
        }
        if (isspecialnolock) na.isdatalock = 'false';
        var isconstraint = (na.isdatalock.toLowerCase() == "true" ? true : false);//islock
        var isunit = (na.ispolicyunit.toLowerCase() == "true" ? true : false);
        var hascansplit = (na.iscansplit.toLowerCase() == "true" ? true : false);
        var hasnewmulti = (getOptions(na.dataid, na.keypath, node.children, true).length > 0 ? true : false), hasnewitem = (getOptions(na.dataid, na.keypath, node.children, false).length > 0 ? true : false);
        var iscannewmulti = (!isinflow && !isfinished && hasnewmulti ? true : false);
        var iscannewitem = (!isinflow && !isfinished && hasnewitem ? true : false);
        var islock = ((isinflow || isconstraint || isfinished) && !isnoflowctrl && (!isspecialnolock) ? true : (isconstraint ? true : false));  //iscanedit || na.keyname == "batchbase"
        var ishalflock = (islock && (iscannewmulti || iscannewitem || iscansplit) ? true : false);
        var iscandelete = (!islock && isoptional ? true : false);   //节点是否能删除，与是否是依赖项没有关系 && !isrely
        var iscanadd = ((!isinflow && !isfinished || ishalflock) && (iscannewmulti || iscannewitem) ? true : false);
        var iscansplit = ((!isinflow && !isfinished || ishalflock) && hascansplit ? true : false);
        var iscanedit = !islock; 
        na.lockAttr = { islock: islock, ishalflock: ishalflock, specialnolock: isspecialnolock, iscandelete: iscandelete, iscanadd: iscanadd, iscanedit: iscanedit, iscansplit: iscansplit, iscannewmulti: iscannewmulti, iscannewitem: iscannewitem, isnoflowctrl: isnoflowctrl };
        na.hasnewmulti = hasnewmulti, na.hasnewitem = hasnewitem;
        var color = (!iscanedit ? (iscannewitem || isfinished && iscansplit ? "Green" : "#a8a7a7") : (isoptional ? "skyblue" : (isrely ? "Green" : (isnoflowctrl ? "blue" : "#000"))));
        
        var stext = node.text;
        switch (na.keyname.toLowerCase()) {
            case "nrbatch":
                break;
            case "dealpackage":
                stext = "Deal政策包 - " + id.substr(id.length - 2);
                break;
            case "vrpackage":
                stext = "VR政策包 - " + id.substr(id.length - 2);
                break;
            default:
                stext = (ismulti ? (stext.indexOf('-') > 0 ? stext.substr(0, stext.indexOf('-') - 1) : stext) + " - " + id.substr(id.length - 2) : stext);
                break;
        }
        node.text = stext;
        //锁按原因类型分为两种：流程锁、数据锁，流程锁是流程的状态对节点数据的修改和删除的约束，数据锁是节点数据被其它对象使用所产生的外键约束（如数据已生成过合同）
        //锁按效力分为全锁、半锁、非锁，全锁和半锁取决于节点的类型
        //全锁：本级和子级都被锁定；半锁：本级可添加新子级，但本级不允许删除，并且本级和现有子级的数据被锁定不允许修改；
        //条件说明：
        //1.锁定判断：isinflow || isfinished || islock，含义：当节点所在的NR批次处于审核流程中  或者  节点所在的NR批次已经完成审核流程  或者  节点受外键约束，则数据处于锁定状态，否则为非锁状态
        //2.锁定状态中再判断全锁还是半锁：iscannewitem || iscansplit，含义：节点处于锁定状态  并且 （节点属于可多次添加的节点 或者  节点属于可拆分的节点），则属于半锁，否则为全锁
        stext = "<a style='color:" + color + "; text-decoration:none;" + (iscanadd ? "font-weight:800;'" : "'") + " href=\"#" + node.id + "\">" + stext + "</a>" + (islock && !ishalflock ? "<img style='vertical-align:bottom' src='../../EasyUI/themes/icons/lock.png' alt='(锁定)' />" : (ishalflock ? "<img style='vertical-align:middle;height:12px;width:12px;' src='../../Images/icon/lockhalf.gif' alt='(锁定)' />" : ""));// + (islock ? "<img style='vertical-align:bottom' src='../../EasyUI/themes/icons/check.png' alt='(锁定)' />" : "");
        if (!isunitpolicy) getnrsection(node);// onmouseover=\"showdatamsg('" + id + "')\" onmouseout=\"hiddatamsg('" + id + "')\"
        var datnewbtn = "toolbar_newchild_" + id;
        if ($('#' + datnewbtn).length > 0) {
            if (hasnewitem){//!iscanedit && iscannewitem || isfinished && iscansplit) {
                $('#' + datnewbtn).removeAttr('disabled');
            } else {
                $('#' + datnewbtn).attr('disabled', 'disabled');
            }
        }
        return stext;
    }
}
function getnrsection(node) {  //根据政策各个分级项目对象的关键字，动态从后台获取政策分级项目的HTML代码，作为政策分级项目的显示和编辑控制区域
    var attr = node.attributes;
    var key = attr.keyname.toLowerCase();
    var id = node.id;
    var url = ""
    var cid = "";
    var parentid = attr.parent;
    var items = getOptions(attr.dataid, attr.keypath);
    var title = "";
    var lockAttr = attr.lockAttr;//{ islock: islock, ishalflock: ishalflock, iscandelete: iscandelete, iscanadd: iscanadd, iscanedit: iscanedit });

    var ismulti = (attr.ismultiple.toLowerCase() == "true" ? true : false);
    var isContainer = (items.length + (node.children ? node.children.length : 0) > 0 ? true : false);
    var isunit = (attr.ispolicyunit.toLowerCase() == "true" ? true : false);
    var islock = lockAttr.islock; //(attr.isdatalock.toLowerCase() == 'true' ? true : false);
    var isoptional = (attr.optional.toLowerCase() == "true" ? true : false);
    var iscansplit = (attr.iscansplit.toLowerCase() == "true" ? true : false);
    var isrely = (attr.relyonnode != "" ? true : false);
    function countpolicy(nod, type) {
        var archd = nod.children;
        var ic = 0;
        if (archd) {
            for (var cc = 0; cc < archd.length; cc++) {
                var k = archd[cc].attributes.keyname.toLowerCase();
                switch (type.toLowerCase()) {
                    case "package":
                        if (k == "dealpackage" || k == "vrpackage") {
                            ic++;
                        }
                        break;
                    case "policy":
                        if (k == "dealall" || k == "vrall" || k == "deala" || k == "vra") {
                            ic++;
                        }
                        break;
                    default:
                        break;
                }
            }
        }
        return ic;
    }
    var ipolicy = 0;
    if (isContainer) {
        switch (key.toLowerCase()) {
            case "nrbatch":
                cid = "";  //不进入数据显示区
                break;
            case "deal":
                cid = "dataarea";
                ipolicy = countpolicy(node, 'package');
                title = "Deal（" + (ipolicy > 0 ? ipolicy : "空") + "）";
                break;
            case "vr":
                cid = "dataarea";
                ipolicy = countpolicy(node, 'package');
                title = "VR政策（" + (ipolicy > 0 ? ipolicy : "空") + "）";
                break;
            case "dealpackage":
                cid = parentid;
                ipolicy = countpolicy(node, 'policy');
                title = "Deal - " + id.substr(id.length - 2) + "（" + (ipolicy > 0 ? ipolicy : "空") + "）";
                break;
            case "vrpackage":
                cid = parentid;
                ipolicy = countpolicy(node, 'policy');
                title = "VR - " + id.substr(id.length - 2) + "（" + (ipolicy > 0 ? ipolicy : "空") + "）";
                break;
            default:
                cid = parentid;
                title = node.text;
                break;
        }
    } else {
        switch (key.toLowerCase()) {
            case "batchbase":
                url = "NRBatch_Base.html";
                cid = "dataarea";
                title = "基本信息";
                break;
            case "dealvdandmedia":
                url = "VendorandMedia.html";
                cid = parentid;
                break;
            case "vrvdandmedia":
                url = "VendorandMedia.html";
                cid = parentid;
                break;
            case "dealallclient":
                url = "DealAllClient.html";
                cid = parentid;
                break;
            case "vrallclient":
                url = "VRAllClient.html";
                cid = parentid;
                break;
            case "dealaclient":
                url = "DealAClient.html";
                cid = parentid;
                break;
            case "vraclient":
                url = "VRAClient.html";
                cid = parentid;
                break;
            case "vrmodel":
                url = "VRCountModel.html";
                cid = parentid;
                break;
            case "vrsummodelaftersplit":
                url = "VRSplitPartsSumCountModel.html";
                cid = parentid;
                break;
            case "paymentterm":
                url = "DealShowOptions.html";
                cid = parentid;
                break;
            case "showoptions":
                url = "DealShowOptions.html";
                cid = parentid;
                break;
            case "priceprotect":
                url = "DealShowOptions.html";
                cid = parentid;
                break;
            case "negativeoptions":
                url = "DealShowOptions.html";
                cid = parentid;
                break;
            case "dealdoc":
                url = "DealDocument.html";
                cid = parentid;
                break;
            case "mv":
                url = "MV.html";
                cid = parentid;
                break;
            case "vrpolicydescription":
                url = "PolicyDescriptTemplate.html";
                cid = parentid;
                break;
            case "vrsample":
                url = "PolicyCountSample.html";
                cid = parentid;
                break;
            case "vrsplitamountbatch":
                url = "VRSplitAmountList.html";
                cid = parentid;
                break;
            case "mvconvertcontractfee":
                url = "MVTransform.html";
                cid = parentid;
                break;
            default:
                cid = parentid;
                break;
        }
    }
    url = segpath + url;
    if (cid != "") {
        var wid = (key != "nrbatch" && key != "batchbase" && cid != "dataarea" ? "p_" : "") + cid;
        var p;
        var indent = 10;  //层次缩进
        var level = getLevel(node);  //节点深度
        var sct = { nodeid: id, containerId: wid, level: level, isContainer: isContainer, isunit: isunit, lockAttr: lockAttr, ismulti: ismulti, isoptional: isoptional, iscansplit: iscansplit, isrely: isrely, keyname: key, parsed: false };
        if (isContainer) {
            sct.title = title;
        } else {
            sct.url = url;
        }
        ar_Section.push(sct); //将数据section参数推入数组缓存起来
        if (key.toLowerCase() == 'vrmodel') {
            ar_CountModel.push(sct);
        //} else {
        }
    }
}
function setupSection() {
    loadmasklayer("正在准备显示数据......");
    if (ar_Section.length > 0) {
        var a = ar_Section[0];
        setTimeout(function () { setupNodeSection(a) }, 0);
    } else {
        if(isFirstLoadMenuTree)isFirstLoadMenuTree = false;
        disLoadmasklayer();
    }
}
function clearStructrue() {
    var nods = $('#menu_nr').tree('getRoots'), i;
    for (i = 0; i < nods.length; i++) {
        var isunit = (nods[i].attributes.ispolicyunit.toLowerCase() == "true" ? true : false);
        if (!isunit) {
            $(nods[i].target).css('display', 'none');
        } else {
            loopchild(nods[i]);
        }
    }
    function loopchild(nod) {
        var children = $('#menu_nr').tree('getChildren', nod.target);
        for (var j = 0; j < children.length; j++) {
            var isunit = (children[j].attributes.ispolicyunit.toLowerCase() == "true" ? true : false);
            if (!isunit) {
                $(children[j].target).css('display', 'none'); //隐藏非政策单元的节点
            } else {
                loopchild(children[j]);
            }
        }
    }
}
function setupNodeSection(a) {  //update at 2016/03/30
    var wid = a.containerId;
    var id = a.nodeid;
    var isContainer = a.isContainer;
    var isunit = a.isunit, isoptional = a.isoptional, iscansplit = a.iscansplit, isrely = a.isrely, ismulti=a.ismulti;
    var level = a.level;
    var key = a.keyname;
    var title = a.title;
    var url = a.url;

    var w = $('#' + wid);
    if ($('#' + wid).length > 0 && w.length > 0) {
        var isExist = ($('#p_' + id).length > 0 ? true : false);
        if (isExist) {
            var ocur = $('#' + (key != "nrbatch" && key != "batchbase" && id != "dataarea" ? "p_" : "") + id);
            var keyname = $($(ocur)[0]).attr('keyname');
            if ($(ocur).length > 0) {
                if (keyname == 'deal' || keyname == 'vr') {
                    //$(ocur).panel("setTitle", title);  //注意对于未定义title属性的panel，会多创建一层title层
                } else {
                    var divheader = $('div.panel-header', $($(ocur)[0]).parent()).length > 0 ? $('div.panel-header', $($(ocur)[0]).parent())[0] : null;
                    if (divheader) {
                        var divtitle = divheader.children[0];
                        $(divtitle).text(title);
                    }
                }
            }
            loopSection(null, a.nodeid, false);
        } else {
            var p;
            var node = $('#menu_nr').tree('find', id), paranode = $('#menu_nr').tree('find', node.attributes.parent);
            var rowkeyId = node.attributes.rowkeyId, prowkeyId = paranode.attributes.rowkeyId;
            var pviewtype = paranode.attributes.viewtype, apview = pviewtype.split(':'), viewtype = node.attributes.viewtype, aview = viewtype.split(':');
            var pkey = paranode.attributes.keyname.toLowerCase();
            var isloadinparent = (viewstyle == '02' && apview[0] == '2' ? true : false);
            var headbar='',toolbar='';
            var lockAttr = node.attributes.lockAttr;//(node.attributes.isdatalock.toLowerCase() == 'true' ? true : false);
            var iscanedit = lockAttr.iscanedit;//(isinflow || islock || isfinished || key == "batchbase" ? false : true);
            iscanedit = (!isoptional && iscanedit && !lockAttr.specialnolock ? !paranode.attributes.lockAttr.islock : iscanedit);  //未锁定的必选项从属于上级节点的锁定状态
            var iscandelete = lockAttr.iscandelete;//( iscanedit && isoptional && !isrely?true:false);
            //var hasnewmulti = node.attributes.hasnewmulti; //(!isinflow && getOptions(node.attributes.dataid,node.children, true).length > 0 ? true : false);
            //var hasnewitem = node.attributes.hasnewitem;
            var iscanadd = lockAttr.iscanadd;
            var iscansplit = lockAttr.iscansplit;
            if (isContainer) {
                if (key != "nrbatch" && key != "batchbase") {
                    var cid = (key != 'batchbase' && key != 'vrsummodelaftersplit' ? "v_" + id : id);
                    var aegischk = "", dentsuchk = "";
                    var conrange = "<span style='display:none;margin:0px;padding:0px;margin-left:50px;font-weight:800;border:0px solid lightgray;font-family:Arial'><label style='display:inline-block;margin:0px 10px 0px 4px'><input id='checkbox_AegisContract_" + cid + "' type='checkbox' " + (lockAttr.islock ? " disabled='disabled' " : '') + " [aegischk] name='AegisContract' />Aegis合约</label><label style='margin:0px 10px 0px 4px;'><input id='checkbox_DentsuContract_" + cid + "' [dentsuchk] type='checkbox' " + (lockAttr.islock ? " disabled='disabled' " : '') + " name='DentsuContract' />DAN合约</label></span>";
                    var rt = getNodeFeature(node);
                    aegischk = (rt.conrange.aegisChk ? "checked='checked'" : (rt.conrange.hasrange && rt.ispub ? "checked='checked'" : ""));
                    dentsuchk = (rt.conrange.aegisChk ? (rt.conrange.hasrange && rt.ispub ? "checked='checked'" : "") : "checked='checked'");
                    conrange = rt.conrange.hasrange ? conrange.replace('[aegischk]', aegischk).replace('[dentsuchk]', dentsuchk) : "";
                    toolbar = (!lockAttr.iscansplit ? "" : "<a href='#' id='toolbar_delete_" + id + "' class='dataoperation' onClick='splitNode(\"" + id + "\")' style='height:25px;'><img src=\"../../EasyUI/themes/icons/tools.ico\" style=\"border:none;margin-top:0px;width:14px;height:14px;vertical-align:middle;\" alt=\"\" />拆分</a>")
                        + (!lockAttr.iscanadd ? "" : "<a href='#' id='toolbar_newchild_" + id + "' class='dataoperation' plain='true' onClick='onContextMenu(event, $(\"#menu_nr\").tree(\"find\", \"" + id + "\"))' style='height:25px;'><img style='width:12px;height:12px;border:none;margin-right:5px;' src='../../EasyUI/themes/icons/edit_add.png' alt=''/>添加...</a>")
                        + (!lockAttr.iscandelete ? "" : "<a href='#' id='toolbar_delete_" + id + "' class='dataoperation' onClick='delNode(\"" + id + "\")' style='height:25px;'><img src=\"../../EasyUI/themes/icons/delete.ico\" style=\"border:none;margin-top:0px;width:14px;height:14px;vertical-align:middle;\" alt=\"\" />删除</a>")
                        + (isunit && rt.policytype == 'd' ? "<span style='height:25px;display:inline-block;'><lable><input id='oldprice_" + id + "' " + (lockAttr.islock ? " disabled='disabled' " : '') + " type='checkbox'/>旧价保护</label></span>" : "");
                    headbar = $("<div id='h_" + id + "' collapse='false'>"
                        + "<span style='display:inline-block;font-weight:800;text-indent:" + (viewIndent && !isloadinparent ? 10 : 0) + "px;border:0px solid green;'><a name='" + id + "'></a>"
                        + "<a href='javascript:void(0)' onclick='sectioncollapse()'><img style='width:12px;height:12px;border:none;margin-right:5px;' src='../../Images/icon/arrow-down.png' alt=''/></a>"
                        + (isunit ? title : "") + "</span>"
                        + (isunit ? conrange : "")
                        + (toolbar == "" ? "" : "<div style='width:auto;position:relative;display:inline-block;height:20px;border:0px solid gray;margin:5px 5px 2px 10px'>" + toolbar + "</div>")
                        + (rowkeyId != undefined && rowkeyId != "-1" || node.attributes.tableFornField != '' ? "<span name='dataid' onmouseover=\"showdatamsg('" + id + "')\" onmouseout=\"hiddatamsg('" + id + "')\" style='display:none; cursor:pointer;margin:0px;padding-left:20px;margin-left:2px;font-weight:600;color:#c0f1ab;width:auto;'>数据编号：" + getIdLink(node.attributes.ParentPrimValue) + "</span>" : "")
                        + "<span title='true' style='display:inline-block;margin:0px;padding-left:20px;margin-left:2px;font-weight:800;color:#05c21d;width:auto;'>路径：" + rt.title + "</span>"
                        + "</div>");
                    var v = $("<div policycontainer='true'></div>");
                    $(v).append(headbar);
                    p = $("<div id='p_" + id + "' keyname='" + key + "' container=true level='" + level + "' data-options='title:\"" + title + "\",collapsible:true' style='margin:1px;margin-left:" + (viewIndent && !isloadinparent ? 10 : 0) + "px;overflow-x:hidden;width: 100%; height: auto; padding: 0px;border:" + (viewIndent && structline ? 1 : 0) + "px solid lightgray;'></div>");
                    $(w).attr('hasContainer', 'true');
                    if (!isloadinparent) {
                        $(w).append(v);
                        w = v;
                    }
                }
            } else {
                var statusbar = "<span title='true' style='display:inline-block;margin:0px;padding:0px;margin-left:2px;margin-bottom:10px;font-weight:800;color:#05c21d;width:auto;'><u>"
                              + (lockAttr.ishalflock ? "<img style='vertical-align:bottom;display:inline-block;' src='../../EasyUI/themes/icons/halflock.png' alt='(锁定)' />"
                               (lockAttr.islock ? "<img style='vertical-align:bottom;display:inline-block;' src='../../EasyUI/themes/icons/check.png' alt='(锁定)' />" : "") : "")
                              + (viewstyle == '01' ? node.text : "")
                              + "</u></span>";
                //var iscanedit = (isinflow || islock || isfinished ? false : true);
                //var iscandelete = (iscanedit && isoptional && !isrely ? true : false);
                //var hasnewmulti = node.attributes.hasnewmulti;
                //var hasnewitem = node.attributes.hasnewitem;
                toolbar = (pkey == "vraftersplit" ? (key == "vrpolicydescription" ? "<a href='#' class='easyui-linkbutton' id='toolbar_edit_" + id + "' onClick='freshBatchSumVr()' data-options='iconCls:\"icon-refresh\",width:65'>刷新</a>" : "") :
                                (!iscanedit ? "" : "<a href='#' " +
                                (key == "batchbase" ? "class='easyui-linkbutton' id='toolbar_edit_" + id + "' onClick='setEdit(\"p\",\"" + id + "\")' data-options='iconCls:\"icon-edit\",width:65'>" : "id='toolbar_dlg_" + id + "' style='margin:3px 5px 2px 5px;' class='dataoperation' onClick='openeditdlg(\"" + id + "\")'>"
                                + "<img src=\"../../EasyUI/themes/icons/pencil.png\" style=\"border:none;margin-top:0px;width:16px;height:15px;vertical-align:middle;\" alt=\"\" />") + "编辑</a>")
                                + (!lockAttr.iscanadd ? "" : "<a href='#' id='toolbar_newchild_" + id + "' class='dataoperation' plain='true' onClick='onContextMenu(event, $(\"#menu_nr\").tree(\"find\", \"" + id + "\"))' style='height:25px;'><img style='width:12px;height:12px;border:none;margin-right:5px;' src='../../EasyUI/themes/icons/edit_add.png' alt=''/>添加...</a>")
                                + (!lockAttr.iscansplit ? "" : "<a href='#' id='toolbar_delete_" + id + "' class='dataoperation' onClick='splitNode(\"" + id + "\")' style='margin:3px 5px 2px 5px;'><img src=\"../../EasyUI/themes/icons/tools.ico\" style=\"border:none;margin-top:0px;width:14px;height:14px;vertical-align:middle;\" alt=\"\" />拆分</a>")
                                + (!lockAttr.iscandelete ? "" : "<a href='#' id='toolbar_delete_" + id + "' class='dataoperation' onClick='delNode(\"" + id + "\")' style='margin:3px 5px 2px 5px;'><img src=\"../../EasyUI/themes/icons/delete.ico\" style=\"border:none;margin-top:0px;width:14px;height:14px;vertical-align:middle;\" alt=\"\" />删除</a>")
                              + (key == "batchbase" ? "<span style='display:none;'><a id='toolbar_cancel_" + id + "' href='#' class='easyui-linkbutton' onClick='dispSectionData(\"p\",\"" + id + "\");mediaoptionsNodeid[" + id + "]=null;' data-options='iconCls:\"icon-cancel\",width:65'>取消</a></span>" : "")
                                );
                headbar = $("<div id='h_" + id + "' section='" + key + "' lock='" + (lockAttr.islock ? 'lock' : '') + "' target='toolbar' style='width:100%;'>"
                          + statusbar
                          + (toolbar == "" || viewstyle == '02' && isloadinparent ? "" : "<div style='position:relative;display:inline-block;height:20px;margin-left:50px;border:0px solid green'>" + toolbar + "</div>")
                          + (rowkeyId != undefined && rowkeyId != "-1" || node.attributes.tableFornField != '' ? "<span name='dataid' onmouseover=\"showdatamsg('" + id + "')\" onmouseout=\"hiddatamsg('" + id + "')\"  style='display:none; cursor:pointer;margin:0px;padding-left:20px;margin-left:2px;font-weight:600;color:#c0f1ab;width:auto;'>数据编号：" + getIdLink(node.attributes.ParentPrimValue) + "</span>" : "") //(prowkeyId == '' || prowkeyId == '0' ? '' : prowkeyId + '->') + rowkeyId
                          + (viewstyle == '01' ? "<br/>" : "")
                          //+ (isunit ? conrange:"")
                          //+ (isunit ? "<span style='margin-left:30px;'><lable><input id='oldprice_" + id + "' disabled='disabled' type='checkbox'/>旧价保护</label></span>":"")
                          + "</div>");

                var v = $("<div policycontainer='true' style=\"margin:1px;margin-left:" + (viewIndent && !isloadinparent ? 1 : 0) + "px;\"><span style='height:2px;width:1px;display:block;border:0px solid green;'><a name='" + id + "'></a></span></div>");
                $(v).append(headbar);
                p = $("<div id='p_" + id + "' keyname='" + key + "' level='" + level + "' style=\"margin:0px;margin-left:0px;overflow-x:hidden;width:auto;height:auto;padding:5px;background:" + viewbkgcolor + ";border-left:0px;border-bottom:1px solid #cdcfd0\"></div>");
                $(w).attr('hasLeaf', 'true');
                if (!isloadinparent) {
                    $(w).append(v);   //用于parser
                    w = v;
                }
            }
            $(w).append(p);
            if (viewstyle == '02') {
                if (apview[0] == '2') {
                    $(p).attr('tabPart', 'tabPage');
                    insertTab({ hasmsg: ($(toolbar).text() == "" ? true : false), msgcontent: toolbar }, paranode.id, p[0], (apview.length > 1 && apview[1] == '1' ? "1" : "0"));  //作为上级Tabs的Tab页
                } 
                if (aview[0] == '2') {
                    $(p).attr('tabPart', 'tabFrame');
                    //加apview[0] != '2'保证工具条对一个节点只创建一次
                    insertTabs({ hasmsg: ($(toolbar).text() == "" && apview[0] != '2' ? true : false), msgcontent: toolbar }, p[0], id, (aview.length > 1 && aview[1] == '1' ? "1" : "0"));  //作为下级对象的Tabs布局容器
                }
            }
            if (key == "batchbase" || key == "vrsummodelaftersplit") {
                //$.parser.onComplete = function (c) { loopSection(c, a.nodeid, true); };
                loopSection(null, a.nodeid, true);
                $.parser.parse($(p).parent());
            } else {
                loopSection(null, a.nodeid, true);
            }
        }
    } else {
        alert('容器“' + wid + ($('#' + wid).length == 0 ? '不存在，可能由于上层容器创建时间过长导致本级容器实际未创建，请刷新页面。' : '”由于上层容器创建超时被忽略创建，请刷新页面重试！'));
    }

}
function getIdLink(link) {
    var arlink = link.split('->');
    var s = '';
    for (var i = 0; i < arlink.length; i++) {
        if (arlink[i].toLowerCase().indexOf('NR005_NRTableDictionaries'.toLowerCase()) < 0) {
            var ar = arlink[i].split('=');
            s += (s == '' ? '' : '>') + (ar.length > 1 ? ar[1] : '');
        }
    }
    return s;
}

function loopSection(c, id, isnew) {  //update at 2016/03/30
    var pos = -1;
    var a;
    for (var i = 0; i < ar_Section.length; i++) {
        if (ar_Section[i].nodeid == id) {
            a = ar_Section[i]; pos = i; break;
        }
    }
    if (!a) return;
    if (!a.parsed) {
        ar_Section[pos].parsed = true;
        if (!a.isContainer && a.url && isnew && (!isFirstLoadMenuTree && a.keyname.toLowerCase() == 'vrmodel' || a.keyname.toLowerCase() != 'vrmodel')) {  //if (key.toLowerCase() == 'vrmodel') {
            getsectiondata(a.nodeid, $('#p_' + a.nodeid), a.url, pos);
        }
    }
    ar_Section.splice(pos, 1);
    if (ar_Section.length > 0) {
        setTimeout(function () { setupNodeSection(ar_Section[0]) }, 0);
    } else {
        //刷新VR汇总政策
        var linkfun = '';
        $.when(asyncEvent()).done(freshSumVr());

        clearStructrue();
        disLoadmasklayer();
    }
    function freshSumVr() {
        freshBatchSumVr();
        if (isFirstLoadMenuTree) isFirstLoadMenuTree = false;
    }
    function asyncEvent(dfd) {
        var dfd = jQuery.Deferred();
        while (ar_CountModel.length>0) {
            var a = ar_CountModel[0];
            if (!a.isContainer && a.url && isnew) {
                dfdfun(i, pos);
            }
            ar_CountModel.splice(0, 1);
        }

        // Return the Promise so caller can't change the Deferred
        return dfd.promise();
        function dfdfun(n, p) {
            var o = ar_CountModel[n];
            getsectiondata(o.nodeid, $('#p_' + o.nodeid), o.url, p);
            dfd.resolve(); // 改变Deferred对象的执行状态
        }
    }
}
function getsectiondata(id, container, url, arIndex) {  //update at 2016/03/30
    if (url == "") return;
    var node = $('#menu_nr').tree('find', id), paranode = $('#menu_nr').tree('find', node.attributes.parent), na = node.attributes;
    var islock = na.lockAttr.islock; //(na.isdatalock.toLowerCase() == 'true' ? true : false);
    var isunit = (na.ispolicyunit.toLowerCase() == 'true' ? true : false);
    var isoptional = (na.optional.toLowerCase() == "true" ? true : false);
    var iscansplit = (na.iscansplit.toLowerCase() == "true" ? true : false);
    var ismulti = (na.ismultiple.toLowerCase() == "true" ? true : false);
    var isrely = (na.relyonnode != "" ? true : false);
    var ourl = (node.attributes.keyname != 'batchbase' && node.attributes.keyname != 'vrsummodelaftersplit' ? url.replace(/([\.|\S]+)(\.html)/gi, "$1" + "_v" + "$2") : url);
    //alert(node.attributes.keyname +';\n\r url:'+url);
    var ft = getNodeFeature(node);
    loadmasklayer("正在读取NR-节点【" + (ft ? ft.path.text : '') + "】的数据......");
    $.ajax({
        url: ourl,
        cache: false,
        success: function (data) {
            var cid = (node.attributes.keyname != 'batchbase' && node.attributes.keyname != 'vrsummodelaftersplit' ? "v_" + id : id);
            var pbody = $(container);
            $(container).attr('href', url);
            var content = $("<div style='width:auto;height:auto;'>" + data + "</div>");
            var key = $(pbody).attr('keyname').toLowerCase(), pkey = paranode.attributes.keyname.toLowerCase();
            content = data.replace(/(\s+id\s*=\s*['|"])(\S+)(['|"]\s*)/gi, "$1" + "$2" + "_" + cid + "$3");  //用分组（分三组元描述id属性数据结构）正则表达式将对话框中的所有id不为空的元素id都加上后缀"id"
            content = content.replace(/(\s+name\s*=\s*['|"])(radio_)(\S+)(['|"]\s*)/gi, "$1" + "$2" + "$3" + "_" + cid + "$4");  //支持name的分组属性
            content = disableContent(content);

            $(container).append(content);
            //初始化或显示NR分层对象的元素值
            if (key == 'vrsummodelaftersplit') {
                var vrmemo = $('textarea #NRV13Memo', container).parent();
                $(vrmemo).hide();
            }
            if (key != "batchbase") {
                var childo = $(content);
                var tabjson = [];
                var maxwidth = 50;  //Tab宽度最小不能小于50px
                $(pbody).find('div[viewtype="tab"]').each(function () {
                    var oe = $(this)[0];
                    var fld = { name: $(oe).attr('title'), title: '<label style="padding-left:10px;padding-right:10px">' + $(oe).attr('title') + '</label>' };
                    var content = oe; var oew = new Number(oe.style.width.replace('px', ''));
                    maxwidth = (maxwidth < oew ? oew : maxwidth);
                    tabjson.push({ field: fld, content: oe, taboptions: { viewstyle:'text', headwidth:'auto',canChecked: false, canClosed: false, visible: true } });
                });
                if (tabjson.length > 0) {
                    var odiv = document.createElement('div');
                    odiv.id = 'tab_' + key + '_' + cid;
                    pbody[0].appendChild(odiv);
                    var o = $Tabs.create(odiv.id, { options: { width: maxwidth + 10, height: 'auto', tabsGap: '2px', contentFrameStyle: 'z-index:1;display:block;border:0px solid gray;border-top:1px solid lightgray' }, tabs: tabjson, onclickTab: 'clicktab' });
                    childo = $(odiv);
                }
                if (document.getElementById('tab_' + id) != undefined) {
                    var tabs = $Tabs.getTabs('tab_' + id), maxwidth = new Number(document.getElementById('tab_' + id).style.width.replace('px', ''));
                    var oe = childo[0];
                    var fld = { name: node.keyname, title: '<label style="padding-left:10px;padding-right:10px">' + node.text + '</label>' };
                    var content = oe; var oew = new Number(oe.style.width.replace('px', ''));
                    maxwidth = (maxwidth < oew ? oew : maxwidth);
                    var tabjson = { field: fld, content: oe, taboptions: { headwidth: '', canChecked: false, canClosed: false, visible: true } };
                    tabs.insertTab(tabs.tabsCount + 1, tabjson);
                    //if (key == "vrsummodelaftersplit") freshBatchSumVr();
                }
                $.parser.parse($(pbody).parent());
                dispSectionData("p", id, true);
            } else {
                $.parser.parse($(pbody).parent());
                initialSegmentPage(key, id);
                dispSectionData("p", id, false);
            }
        }
        , error: function (err) {
            alert(err.responseText);
        }
    });
}
function sectioncollapse(type) {
    var oe = (event.srcElement ? event.srcElement : event.target);
    if (oe.tagName.toLowerCase() == 'img') oe = oe.parentNode;
    var img = oe.firstChild;
    var headsect = oe.parentNode.parentNode; //一样的
    var clpdiv = $(headsect.parentNode).find('div[keyname]:first');
    clpdiv = (clpdiv.length == 0 ? $(headsect.parentNode).find('div[collapse]:first') : clpdiv);
    var datasect = (type == 'docdiv' ? $(headsect).find('table')[0] : clpdiv[0]);
    var b = $(headsect).attr('collapse') == 'true' ? true : false;
    if (b) {
        img.src = '../../Images/icon/arrow-down.png';
        $(datasect).css('display', 'block');
        $(headsect).attr('collapse', 'false');
    } else {
        img.src = '../../Images/icon/arrow-right.png';
        $(datasect).css('display', 'none');
        $(headsect).attr('collapse', 'true');
    }

}
function getNodeFeature(node) {
    var isrange = false, isaegis = false;
    var istop = false, ispublic = true;
    var plctype;
    var sttl = "[" + node.text + "]";
    var rawnode = node;
    var st = node.text;
    var sid = node.id;
    while (!istop && node != null) {
        node = $("#menu_nr").tree('getParent', node.target);
        if (node != null) {
            var k = node.attributes.keyname.toLowerCase();
            st = node.text + (st == "" ? "" : "=>") + st;
            sid = node.id + (sid == "" ? "" : "-") + sid;
            var imgsplit = "<img src='../../Images/icon/arrow-right1.gif' alt='=>' style='margin-top:0px;width:16px;height:15px;vertical-align:middle;' />";
            sttl = "[" + node.text + "]" + (sttl == "" ? "" : imgsplit) + sttl;
            if (k == "dealpackage" || k == "vrpackage" || k == "dataarea" || k == "nrbatch" || k == "batchbase") {
                istop = true;
            }
            if (k == "vrpackage") {  //当前节点属于VR类型的节点，需要添加合约类型（除了拆分政策，其它的所有VR政策的签约类型都设置为DAN合约）
                isrange = true; 
            }
            plctype = (k == "dealpackage" ? 'd' : (k == "vrpackage" ? "v" : ""));
            //vrsplitperiod,sourcesplitperiod
            if (!isaegis && (k == "vrsplitperiod" || k == "sourcesplitperiod" || k == "vraftersplit")) {//(k == 'VRa' || k == 'vrpartcollection' || k == 'VRaftersplit')
                isaegis = true;
            }
            if (ispublic) ispublic = false;
        }
    }
    rawnode.attributes.path = { id: sid, text: st };

    return { policytype: plctype, path: rawnode.attributes.path, title: sttl, ispub: ispublic, conrange: { hasrange: isrange, aegisChk: isaegis } };
}
function getNodeHtml(data) {
    var pattern = /<body[^>]*>((.|[\n\r])*)<\/body>/im;
    var matches = pattern.exec(data);
    if (matches) {
        return matches[1];	// 仅提取主体内容
    } else {
        return data;
    }
}

function disableContent(ct) {
    var pat = "(<\\s*input|select)([\\S|\\s]+?)(class\\s*=\\s*['|\"])([\\S|\\s]*?)(easyui-[^'\"]+\\s*?)([\\S|\\s]*?)(['|\"]\\s*)([^(data-options)]*?)";//  ^((?!data-options)\\w)
    var optpat = pat + "(data-options\\s*=\\s*)(['|\"])([\\S|\\s]*?)(\\10)([\\S|\\s]*?)(\\/>|>)";
    var nonoptpat = pat + "(\/>|>)";
    var reg1 = new RegExp(optpat, 'gi');
    var reg2 = new RegExp(nonoptpat, 'gi');
    ct = ct.replace(reg1, "$1" + "$2" + "$3" + "$4" + "$5" + "$6" + "$7" + "$8" + "$9" + "$10" + "disabled:true," + "$11" + "$12" + "$13" + "$14");
    ct = ct.replace(reg2, "$1" + "$2" + "$3" + "$4" + "$5" + "$6" + "$7" + "$8" + " data-options='disabled:true' " + "$9");
    return ct;
}
function setEdit(prefix, id) {
    if (!id) return;
    var node = $('#menu_nr').tree('find', id);
    var islock = node.attributes.lockAttr.islock;
    if (islock) {
        alert('数据被锁定，禁止编辑！');
        return;
    }
    if ($('#toolbar_edit_' + id).text().replace(String.fromCharCode(160), '') == "保存") {
        saveSectionData(prefix, id);
    } else {
        enableSegEles(prefix, id, true);
    }
}
function enableSegEles(prefix, id, valid) {
    var ctnid = prefix + '_' + id;
    var op = $('#' + ctnid);
    if (op.length > 0) {
        var key = op.attr('keyname');
        $("#" + ctnid + " input[class^='easyui-']:not([preserveEnable]),#" + ctnid + " a[class^='easyui-']:not([preserveEnable])").each(function () {
            var cls = $(this).attr('class');
            cls = cls.substr(cls.indexOf('-') + 1);
            cls = cls.indexOf(' ') >= 0 ? cls.substr(0, cls.indexOf(' ')) : cls;
            enordisEasyEle(this, cls, valid);
        });
        enableNonEasyEle(ctnid, valid);
        $("#" + ctnid + " input[class^='easyui-'][preserveEnable],#" + ctnid + " a[class^='easyui-'][preserveEnable]").each(function () {
            var cls = $(this).attr('class');
            if (cls != null && cls != undefined) {
                cls = cls.substr(cls.indexOf('-') + 1);
                cls = cls.indexOf(' ') >= 0 ? cls.substr(0, cls.indexOf(' ')) : cls;
                enordisEasyEle(this, cls, true);
            }
        });
        
    }
    $('#toolbar_edit_' + id).linkbutton({ "text": (valid ? "保存" : "编辑"), "iconCls": (valid ? "icon-save" : "icon-edit") });
    $('#toolbar_cancel_' + id).linkbutton((valid ? "enable" : "disable")).parent().css('display', (valid ? "inline-block" : 'none'));
    clearTimeout(timeoutID);
}
function enableNonEasyEle(ctnid, valid) {
    var artype = new Array('input', 'select', 'a', 'textarea');
    var selor = "", readonlyselor = "";
    for (var i = 0; i < artype.length; i++) {
        selor += (selor == "" ? "" : ",") + artype[i] + ":not([class]):not([preserveEnable])," + artype[i] + ":not([class^='easyui-']):not([preserveEnable])";
        readonlyselor += (readonlyselor == "" ? "" : ",") + artype[i] + ":not([class])[preserveEnable]," + artype[i] + ":not([class^='easyui-'])[preserveEnable]";
    }
    $(selor, $("#" + ctnid)).each(function () {
        if (!valid) {
            $(this).attr('disabled', 'disabled');
        } else {
            $(this).removeAttr('disabled');
        }
    });
    $(readonlyselor, $("#" + ctnid)).each(function () {
        $(this).removeAttr('disabled');
    });
}
function enordisEasyEle(easyObj, easyCls, valid) {
    switch (easyCls.toLowerCase()) {
        case "textbox":
            $(easyObj).textbox(valid ? 'enable' : 'disable');
            break;
        case "validatebox":
            $(easyObj).validatebox(valid ? 'enable' : 'disable');
            break;
        case "passwordbox":
            $(easyObj).passwordbox(valid ? 'enable' : 'disable');
            break;
        case "combo":
            $(easyObj).combo(valid ? 'enable' : 'disable');
            break;
        case "combobox":
            $(easyObj).combobox(valid ? 'enable' : 'disable');
            break;
        case "combotree":
            $(easyObj).combotree(valid ? 'enable' : 'disable');
            break;
        case "combogrid":
            $(easyObj).combogrid(valid ? 'enable' : 'disable');
            break;
        case "combotreegrid":
            $(easyObj).combotreegrid(valid ? 'enable' : 'disable');
            break;
        case "numberbox":
            $(easyObj).numberbox(valid ? 'enable' : 'disable');
            break;
        case "datebox":
            $(easyObj).datebox(valid ? 'enable' : 'disable');
            break;
        case "datetimebox":
            $(easyObj).datetimebox(valid ? 'enable' : 'disable');
            break;
        case "datetimespinner":
            $(easyObj).datetimespinner(valid ? 'enable' : 'disable');
            break;
        case "calendar":
            $(easyObj).calendar(valid ? 'enable' : 'disable');
            break;
        case "spinner":
            $(easyObj).spinner(valid ? 'enable' : 'disable');
            break;
        case "numberspinner":
            $(easyObj).numberspinner(valid ? 'enable' : 'disable');
            break;
        case "timespinner":
            $(easyObj).timespinner(valid ? 'enable' : 'disable');
            break;
        case "menu":
            $(easyObj).menu(valid ? 'enable' : 'disable');
            break;
        case "linkbutton":
            $(easyObj).linkbutton(valid ? 'enable' : 'disable');
            break;
        case "menubutton":
            $(easyObj).menubutton(valid ? 'enable' : 'disable');
            break;
        case "splitbutton":
            $(easyObj).splitbutton(valid ? 'enable' : 'disable');
            break;
        case "switchbutton":
            $(easyObj).switchbutton(valid ? 'enable' : 'disable');
            break;
    }
}
function freshSectionView(id) {
    //初始化或显示NR分层对象的元素值
    var node = $('#menu_nr').tree('find', id);
    var key = node.attributes.keyname;
    if (key != "batchbase") {
        dispSectionData("p", id, true);
        $('#dlg_edit').dialog('close');
    } else {
        initialSegmentPage(key, id);
        dispSectionData("p", id, false);
    }
}
function saveSectionData(prefix, id, saveinview) {
    var node = $('#menu_nr').tree('find', id);
    var key = node.attributes.keyname;
    //这里添加NR数据节点的数据保存处理
    //以changedNodes为参数保存节点及其数据
    //为兼顾数据完整性控制，保存有一定的层级顺序：先上级后下级;同级的强制节点则用TAB进行包装，并共享一个保存按钮
    NegoBatch.Excute("saveSementPageData", id, key, saveinview);

    if (saveinview || key == "batchbase") {
        //完成保存,disable元素
        enableSegEles(prefix, id, false);
        //} else {
        //    freshSectionView(id);
    }
}
var timeoutID;
function dispSectionData(prefix, id, isView) {  //update at 2016/03/30
    //执行初始化界面元素
    NegoBatch.Excute("InitSegmentPageData", id, null, (isView ? true : false));
    if (!isView) {
        enableSegEles(prefix, id, false);
    }
    //完成显示,disable非easyUI元素
    //  timeoutID = setTimeout("enableSegEles(" + id + ", false)", 2000)

}
function openeditdlg(id) {  //update at 2016/03/30
    //var oe = event.srcElement.parentNode.parentNode;
    if (!id) return;
    var node = $('#menu_nr').tree('find', id), paranode = $('#menu_nr').tree('find', node.attributes.parent);
    var na = node.attributes;
    var islock = na.lockAttr.islock; //(na.isdatalock.toLowerCase() == 'true' ? true : false);
    var isunit = (na.ispolicyunit.toLowerCase() == 'true' ? true : false);
    if (isinflow || !isinflow && islock) {
        alert('数据被锁定，禁止编辑！');
        return;
    }

    var nrid = $.trim($("#nrId").val());
    if (nrid == '' || nrid == '0') {
        alert('请先保存当前NR批次的基本信息');
        return;
    }

    var mediaseltype = '';
    var mediaoptions = '';
    var mediasStr = '';
    var oe = (event.srcElement ? event.srcElement : event.target);
    while (oe = oe.parentNode) {
        if (oe.keyname && (oe.keyname == 'dealpackage' || oe.keyname == 'vrpackage')) {
            $(oe).find("label[id^='mediaseltype']").each(function () { mediaseltype = $(this).text(); });
            $(oe).find("label[id^='mediaoptions']").each(function () { mediaoptions = $(this).text(); });
            $(oe).find("input[type='hidden'][id^='mediasStr']").each(function () { mediasStr = $(this).val(); });
            break;
        }
    }

    //首先根据节点id：nodeid，找到div层
    //再在div层中寻找需要的元素，例如：想找VR计算模型中表格对象
    //先找到当前节点对应的计算模型的div层：odiv
    //再在odiv中寻找table元素：
    //var str = $(odiv).find("table[id^='diffmodelstage']")[0].html;
    //

    var ft = getNodeFeature(node);
    var pid = 'p_dlg_' + id;
    var op = $('#p_' + id);
    var key = op.attr('keyname').toLowerCase(), pkey = paranode.attributes.keyname.toLowerCase();
    var url = op.attr('href');
    var odlg = $('#dlg_edit');
    var pbody = $(odlg).panel('body');
    if (url && pbody) {
        loadmasklayer('');
        $(odlg).panel('clear');
        $.ajax({
            url: url,
            cache: false,
            success: function (data) {
                var container = odlg;
                $(odlg).attr('href', url);
                var content = $(data);
                content = data.replace(/(\s+id\s*=\s*['|"])(\S+)(['|"]\s*)/gi, "$1" + "$2" + "_" + id + "$3");  //用分组（分三组元描述id属性数据结构）正则表达式将对话框中的所有id不为空的元素id都加上后缀"id"
                content = content.replace(/(\s+name\s*=\s*['|"])(radio_)(\S+)(['|"]\s*)/gi, "$1" + "$2" + "$3" + "_" + id + "$4");  //支持name的分组属性
                content = disableContent(content);
                var aegischk = "", dentsuchk = "";
                var conrange = "<span style='display:inline-block;margin:0px;padding:0px;margin-left:20px;width:200px;height:20px;font-weight:800;border:1px solid gray;background-color:lightgray;font-family:Arial'><label style='margin:0px 10px 0px 4px'><input id='checkbox_AegisContract_" + id + "' type='checkbox' [aegischk] name='AegisContract' />Aegis合约</label><label style='margin:0px 10px 0px 4px;'><input id='checkbox_DentsuContract_" + id + "' [dentsuchk] type='checkbox' name='DentsuContract' />DAN合约</label></span>";
                var rt = ft;
                aegischk = (rt.conrange.aegisChk ? "checked='checked'" : (rt.conrange.hasrange && rt.ispub ? "checked='checked'" : ""));
                dentsuchk = (rt.conrange.aegisChk ? (rt.conrange.hasrange && rt.ispub ? "checked='checked'" : "") : "checked='checked'");
                conrange = rt.conrange.hasrange ? conrange.replace('[aegischk]', aegischk).replace('[dentsuchk]', dentsuchk) : "";
                var toolbar = "<div section='" + key + "' target='toolbar' style='width:100%;'>"
                            + "<span title='true' style='display:inline-block;margin:0px;padding:0px;margin-left:20px;font-weight:800;color:#05c21d;width:500px;height:20px;'>" + rt.title + "</span>"
                            + "<br/>"
                            + (key == "dealvdandmedia" || key == "vrvdandmedia" ? "" : "<div>媒体选择方式：<label style=\"width: 200px\" class=\"stringValue\" >" + mediaseltype + "</label>【<label style=\"width: 300px\" class=\"stringValue\">" + mediaoptions + "</label>】</div>"
                            + "<div>媒体列表：<label style=\"width: 200px\" class=\"stringValue\" >" + mediasStr + "</label></div>"
                            + "<br/>")
                            + (pkey == "vraftersplit" ? (key == "vrpolicydescription" ? "<a href='#' class='easyui-linkbutton' id='toolbar_edit_" + id + "' onClick='freshBatchSumVr()' data-options='iconCls:\"icon-refresh\",width:65'>刷新</a>" : "") :
                                "<a preserveEnable id='toolbar_edit_" + id + "' href='#' class='easyui-linkbutton' onClick='setEdit(\"p_dlg\",\"" + id + "\")' data-options='iconCls:\"icon-edit\",width:65'>编辑</a>"
                                + "<span style='display:none;'><a id='toolbar_cancel_" + id + "' href='#' class='easyui-linkbutton' onClick='dispSectionData(\"p_dlg\",\"" + id + "\");mediaoptionsNodeid[" + id + "]=null;' data-options='iconCls:\"icon-cancel\",width:65'>取消</a></span>"
                                )
                            + (isunit ? conrange : "")
                            + (isunit ? "<span style='margin-left:30px;'><lable><input id='oldprice_" + id + "' disabled='disabled' type='checkbox'/>旧价保护</label></span>" : "")
                            + "</div>";
                //$(container).append(toolbar);
                //$(container).append(content);
                pbody.html("<div id=" + pid + " style='margin:10px;'>" + toolbar + content + "</div>");
                $.parser.parse(odlg);
                //$.parser.parse($(pbody).parent());
                //初始化或显示NR分层对象的元素值
                initialSegmentPage(key, id);
                dispSectionData("p_dlg", id, false);
                var size = { 'width': 100, 'height': 100 };
                switch (key.toLowerCase()) {
                    case "batchbase":
                        size.width = 1000; size.height = 500;
                        break;
                    case "dealvdandmedia":
                        size.width = 850; size.height = 480;
                        break;
                    case "vrvdandmedia":
                        size.width = 850; size.height = 480;
                        break;
                    case "dealallclient":
                        size.width = 900; size.height = 400;
                        break;
                    case "vrallclient":
                        size.width = 900; size.height = 400;
                        break;
                    case "dealaclient":
                        size.width = 1000; size.height = 450;
                        break;
                    case "vraclient":
                        size.width = 1000; size.height = 450;
                        break;
                    case "vrmodel":
                        size.width = 750; size.height = 500;
                        break;
                    case "vrsummodelaftersplit":
                        size.width = 1000; size.height = 500;
                        break;
                    case "paymentterm":
                        size.width = 700; size.height = 350;
                        break;
                    case "showoptions":
                        size.width = 700; size.height = 400;
                        break;
                    case "priceprotect":
                        size.width = 1000; size.height = 500;
                        break;
                    case "negativeoptions":
                        size.width = 1000; size.height = 500;
                        break;
                    case "dealdoc":
                        size.width = 900; size.height = 400;
                        break;
                    case "mv":
                        size.width = 1000; size.height = 500;
                        break;
                    case "vrpolicydescription":
                        size.width = 750; size.height = 500;
                        break;
                    case "vrsample":
                        size.width = 1000; size.height = 500;
                        break;
                    case "vrsplitamountbatch":
                        size.width = 1000; size.height = 500;
                        break;
                    default:
                        size.width = 1000; size.height = 500;
                        break;
                }
                $(odlg).dialog({
                    title: '编辑政策', 'width': size.width, 'height': size.height, 'top': ($(window).height() - size.height) * 0.5,
                    left: ($(window).width() - size.width) * 0.5,
                    onOpen: function () {
                        $("<div class=\"datagrid-mask\"></div>").css({ display: "none", width: "100%", height: "100%" }).appendTo(pbody);
                        $("<div class=\"datagrid-mask-msg\"></div>").appendTo(pbody).css({ display: "none", left: ($(odlg).outerWidth(true) - 200) / 2, top: ($(odlg).height() - 45) / 2 });
                    },
                    onClose: function () {
                        disLoadmasklayer($("body"));
                    }
                }).dialog('open');
            }
        });
    }
}
function initialSegmentPage(key, pid) {
    //初始化页面控件
    NegoBatch.Excute('InitSegmentPageControl', pid, key);

    if (key == 'batchbase') {
        //给附件列表添加删除事件
        document.getElementById('DelAccessoryBtn' + '_' + pid).onclick = function () { DelAccessory(pid); }
        //给附件列表添加刷新事件
        document.getElementById('RefreshAccessoryBtn' + '_' + pid).onclick = function () { RefreshAccessory(pid); }
        //给附件列表添加增加事件
        document.getElementById('AddAccessoryBtn' + '_' + pid).onclick = function () { AddAccessory(pid); }
    }
}

function getLevel(node) {
    var rt = $('#menu_nr').tree('getRoot');
    var l = 1;
    while (rt != node) {
        node = $('#menu_nr').tree('find', node.attributes.parent);
        l++;
    }
    return l;
}
function countLevel(children, findid, curLevel) {
    var p = { rst: false, level: curLevel + 1 };
    if (children) {
        for (var i = 0; i < children.length; i++) {
            if (children[i].id == findid) {
                p.rst = true;
                break;
            } else {
                if (children[i].children) {
                    p = countLevel(children[i].children, findid, p.level);
                    if (p.rst) break;
                }
            }
        }
    }
    return p;
}
function getOptions(parentid,parentkeypath,curchild,onlymulti) {  //获取可选政策项
    var rtn = new Array();
    if (optJson) {
        for (var i = 0; i < optJson.length; i++) {
            var optatr=optJson[i].attributes;
            var key = ',' + optatr.dataparent + ',';
            if (key.indexOf(',' + parentid + ',') >= 0 && ((onlymulti == undefined || onlymulti == false) && !existsInChild(optatr.dataid, curchild) || onlymulti == true && optatr.ismultiple.toLowerCase() == 'true')) {
                var opt = optJson[i];
                opt.attributes.keypath = (parentkeypath != undefined && parentkeypath != null && parentkeypath != '' ? parentkeypath + '-' : '') + opt.attributes.keypath;   //根据父项确定实体keypath的值
                rtn.push(opt);
            }
        }
    }
    return rtn;
}
function existsInChild(id, child) {
    if (child == undefined) return false;
    for (var i = 0; i < child.length; i++) {
        var at = child[i].attributes;
        if (at.dataid == id) {
            if (at.ismultiple.toLowerCase() == 'true') return false;
            return true;
        }
    }
    return false;
}
function onContextMenu(e, node) {  //激发弹出式菜单
    try{
        e.preventDefault();}catch(er){}
    $('#menu_nr').tree('select', node.id);
    var pnode = $('#menu_nr').tree('getParent', node.target);
    var mnode = node;
    if (mnode && mnode.attributes && mnode.attributes.keyname) {
        var mtype = mnode.attributes.keyname;
        if (mtype) {
            createpopmenu(mnode,pnode);
            $('#mm').menu('show', {
                left: (e.pageX?e.pageX:e.clientX),
                top: (e.pageY ? e.pageY : e.clientY)
            });
        }
    }
}

function createpopmenu(node, pnode) {  //创建可选政策项目的动态子菜单
    var na = node.attributes;
    var curchild = node.children;
    var isleaf = $(this).tree('isLeaf', node.target);

    var nr001ID = $('#NR001Id').val();

    var isoptional = (na.optional.toLowerCase() == "true" ? true : false);
    var iscansplit = (na.iscansplit.toLowerCase() == "true" ? true : false);
    var isrely = (na.relyonnode != "" ? true : false);
    var isunit = (na.ispolicyunit.toLowerCase() == "true" ? true : false);
    var ismulti = transbool(na.ismultiple);
    var islock = na.lockAttr.islock; //(na.isdatalock.toLowerCase() == 'true' ? true : false);
    var ishalflock = na.lockAttr.ishalflock;

    var iscanedit = na.lockAttr.iscanedit;//(isinflow || islock || isfinished || na.keyname == "batchbase" ? false : true);
    var iscandelete = na.lockAttr.iscandelete;//(iscanedit && isoptional && !isrely ? true : false);
    var iscannewmulti = (!isinflow && !isfinished && na.lockAttr.iscannewmulti ? true : false);
    var iscannewitem = (!isinflow && !isfinished && na.lockAttr.iscannewitem ? true : false);

    var smenu = "";
    //if (pnode && pnode.attributes && pnode.attributes.keyname == 'VRa') {//单品政策禁止政策拆分操作
    //    iscansplit = false;
    //}
    var items = (!islock || ishalflock ? getOptions(na.dataid, na.keypath, curchild, false) : []);  //对于第二参数=false时，表示包含：可选多次项+可选未选项;否则，就是半锁状态，只能添加可重复添加的选项，不包括可选未选项


    //动态添加子菜单
    //menu插件的item参数不支持attributes属性，所以需要将json中的attributes转换到menuitem的name属性中去
    var parent = $('#mm').menu('findItem', '添加政策项目');
    if (parent) {
        var submenus = $('#mm').menu('getSubmenu', parent.target);
        subs = submenus ? $('div.menu-item', submenus) : null;
        if (subs) {
            for (var i = 0; i < subs.length ; i++) {
                $('#mm').menu('removeItem', subs[i]);
            }
        }
        $('#mm').menu((items.length == 0 ? 'hideItem' : 'showItem'), parent.target);
        var sepmenu = $(".menu-sep", $(parent.target).parent());
        if (sepmenu && sepmenu.length > 0) $('#mm').menu((items.length == 0 ? 'hideItem' : 'showItem'), sepmenu[0]);
        for (var i = 0; i < items.length; i++) {
            var itm = items[i];
            var a = itm.attributes;
            var oitem = {
                parent: parent.target,  // 设置父菜单元素
                id: 'm_add_' + itm.id,  //加“m_”前缀，避免与同源对象id重复
                text: "添加【" + itm.text + "】",
                name: {
                    popnode: node,//激发弹出菜单的树节点
                    command: 'add',
                    keyname: a.keyname,
                    optional: a.optional,
                    ismulti: a.ismultiple,
                    iscansplit: a.iscansplit,
                    isdatalock: a.isdatalock,
                    relyonnode: a.relyonnode,
                    noflowcontrol:a.noflowcontrol,
                    dataid: a.dataid,
                    parent: a.parent,
                    nr005id: a.nr005id,
                    dataparent: a.dataparent,
                    html: a.html,
                }
            };
            var mn = $('#mm').menu('appendItem', oitem);
        }
        var itemname = {
            popnode: node,
            command: '',
            keyname: na.keyname,
            optional: na.optional,
            ismulti: na.ismultiple,
            iscansplit: na.iscansplit,
            isdatalock: na.isdatalock,
            relyonnode: na.relyonnode,
            noflowcontrol: na.noflowcontrol,
            dataid: na.dataid,
            parent: na.parent,
            nr005id: na.nr005id,
            dataparent: na.dataparent,
            html: na.html
        };
        function insertmenuitem(node, dotype, groupkey, items, isaddsep) {
            if ($('#m_' + groupkey + '_sep')[0]) {
                var oitemp = $('#mm').menu('getItem', $('#m_' + groupkey + '_sep')[0]);
                if (oitemp) $('#mm').menu('removeItem', oitemp.target);
            }
            for (var i = 0; i < items.length; i++) {
                var idkey = items[i].idkey;
                var itemtext = items[i].itemtext;
                var oitem = $('#mm').menu('findItem', itemtext);
                if (oitem) $('#mm').menu('removeItem', oitem.target);
                if (dotype.toLowerCase() == "display") {
                    var _name = $.extend(true, {}, itemname);  //复制name属性
                    _name.command = idkey;
                    $('#mm').menu('appendItem', {
                        id: 'm_' + idkey + '_' + node.id,
                        iconCls: items[i].iconCls,
                        text: itemtext,
                        name: _name
                    });
                }
            }
            if (dotype.toLowerCase() == "display" && isaddsep) {
                $('#mm').menu('appendItem', {
                    separator: true,
                    id: 'm_' + groupkey + '_sep'
                });
            }
        }

        if (node.text == '客户')
        {          
            var url = '../Handler/NRPolicy/GetNRPolicyHandler.ashx?JobName=GetNROldPolicy&NodeID=' + node.id + '&NR001ID=' + nr001ID;
            $.ajax({
                url: url,
                async: false,
                cache: false,
                success: function (data) {
                    var oitem = $('#mm').menu('findItem', isOldPolicyProtected);
                    if (oitem) $('#mm').menu('removeItem', oitem.target);
                    if (data == 'True')
                    {
                        isOldPolicyProtected = '取消旧价保护';
                        idkey = 'canceloldprices';
                    }
                    else if (data == 'False')
                    {
                        isOldPolicyProtected = '旧价保护';
                        idkey = 'oldprices';
                    }
                }
            })
        }
        
        insertmenuitem(node, (node.text == '客户'?"display":"hide"), 'protect', [{ idkey: idkey, itemtext: isOldPolicyProtected }], false);
        insertmenuitem(node, (!isinflow && iscansplit ? "display" : "hide"), 'split', [{ idkey: 'split', iconCls: 'icon-tools', itemtext: '添加政策期间' }], true);
        insertmenuitem(node, ((isoptional || isrely) && iscandelete ? "display" : "hide"), 'del', [{ idkey: 'delete', iconCls: 'icon-empty', itemtext: '删除政策项目' }], true);
        insertmenuitem(node, (isleaf ? "hide" : "display"), 'clp_exp', [{ idkey: 'collapse', itemtext: '折叠' }, { idkey: 'expand', itemtext: '展开' }], true);
        insertmenuitem(node, (iscanedit ? "display" : "hide"), 'import', [{ idkey: 'imfromms', iconCls: 'icon-import', itemtext: '从Media System导入政策' }, { idkey: 'imfromnr', iconCls: 'icon-import', itemtext: '从NR Application导入政策' }], false);
        //if (isunit) insertmenuitem(node, "display", 'import', [{ idkey: 'oldprices', itemtext: '旧价保护' }], false);

        $.parser.parse(parent.target);
    }

}

function menuhandler(menuitem) {
    var node = menuitem.name.popnode;
    //对于已提交到流程尚未结束的NR批次，禁止对所有节点的增删改操作，此时根据流程状态来控制，无需特别设置isdatalock
    //对于已结束流程的NR批次：除非重启NR流程，否则同样禁止对所有节点的增删改操作（还要看合同生成情况）
    //对于已生成合同、并且已重启流程的NR批次：
    //1.生成合同的节点：将被加锁（isdatalock=true），不能做删除和修改操作，对于添加子节点或拆分节点的操作，则要看noflowcontrol(是否为受流程控制的节点)，true则可以false则不行；
    //2.未生成合同的节点:可以任意操作
    var islock = node.attributes.lockAttr.islock; //(node.attributes.isdatalock.toLowerCase() == 'true' ? true : false);
    var noflowcontrol = node.attributes.noflowcontrol;//.toLowerCase() == 'true' ? true : false);
    if (menuitem.name.command) {
        switch (menuitem.name.command.toLowerCase()) {
            case "add":
                var popnode = menuitem.name.popnode;
                addPolicy(popnode.id, { dataid: menuitem.name.dataid, keyname: menuitem.name.keyname, keypath: node.attributes.keypath, ismulti: menuitem.name.ismulti });
                break;
            case "split":
                splitNode(node.id);
                break;
            case "delete":
                delNode(node.id);
                break;
            case "collapse":
                collapse(node);
                break;
            case "expand":
                expand(node);
                break;
            case "imfromms":
                //添加导入media system政策的代码
                break;
            case "imfromnr":
                //添加导入NR政策的代码
                break;
            case "oldprices":
            case "canceloldprices":
                //旧价保护
                   var nR001ID = $('#NR001Id').val();
                   var nodeid = menuitem.name.popnode.id;
                   var isOldprice = (menuitem.text == '旧价保护') ? 1 : 0;
                   //var obj = (menuitem.name.keyname == 'Dealallclient') ? $('#lb_oldPriceComon' + '_' + nodeid) : $('#lb_oldPrice' + '_' + nodeid);
                   var obj;
                   switch(menuitem.name.keyname)
                   {
                       case 'Dealallclient':
                           obj = $('#lb_DealAlloldPrice' + '_' + nodeid);
                           break;
                       case 'Dealaclient':
                           obj = $('#lb_DealAoldPrice' + '_' + nodeid);
                           break;
                       case 'VRaclient':
                           obj = $('#lb_VRAoldPrice' + '_' + nodeid);
                           break;
                       case 'VRallclient':
                           obj = $('#lb_VRAlloldPrice' + '_' + nodeid);
                           break;

                   }
                   
                    $.post("../../Handler/NRPolicy/SaveNRPolicyHandler.ashx?JobName=EditNRNodeOldPricePolicy",
                    {
                        NRBatchID: nR001ID,
                        nodeid:nodeid,
                        isProtectOldPrice: isOldprice
                    }
                     , function (data) {
                         if (data.length == 0)
                         {
                             if (isOldprice == 1) {
                                 if (obj) obj.css('display', 'block');
                             }
                             else if (isOldprice == 0)
                             {
                                 if (obj) obj.css('display', 'none');
                             }
                         }
                     });
                break;
        }
    }
}
function transbool(val) {
    return (val.toLowerCase() == "true" ? true : false);
}
//eNode:激发依赖处理事件的节点；relies:依赖项集合；addeds:已经添加过的依赖关联组合(节点+依赖项)
var tmpR = { eNode: null, relies: new Array(), addeds: new Array(), lastIndex: 0 };

function resizedataarea() {
    //处理滚动条导致panel宽度不适应的问题
    var margin = 10;
    var divscroll = $('#dataarea')[0];
    var headoffset = 10;
    var c1 = '.easyui-fluid', c2 = '.panel-header:first', c3 = '.panel-body:first';
    function resizepanel(opanel, width, headoffset) {
        var phead = $(c1 + ' ' + c2, opanel);
        var pbody = $(c1 + ' ' + c3, opanel);
        var l = Number($(pbody).attr('level'));
        if (phead && $(pbody).attr('keyname') && $(pbody).attr('keyname') != "") {  //有head，并且body有keyname定义
            $(pbody).css('width', (width - headoffset * l) + 'px');
            $(phead).css('width', (width - headoffset * l) + 'px');
            if ($('.panel ' + c1, pbody).length > 0)
                $('.panel ' + c1, pbody).each(function () { resizepanel(this, width, headoffset); });
        }
    }
    $('#dataarea').children().each(function () {  //panel数据分区
        var w = divscroll.clientWidth;
        var l = $('div.easyui-panel[level]:first', this);
        $(c1 + ' ' + c3, this).css('width', (w - headoffset * l) + 'px');
        $(c1 + ' ' + c2, this).css('width', (w - headoffset * l) + 'px');
        $('.panel ' + c1, $(c1 + ' ' + c3, this)).each(function () {
            resizepanel(this, w, headoffset);
        });
    });
}
function removeNode(node) {
    if (node) {
        var na = node.attributes;
        var key = na.keyname;
        var curnode = node;
        if (na) {
            var id = na.dataid;
            if (na.relyonnode) {
                if (na.relyonnode != "") clearRelyRec(curnode, id);
            }
            function clearRelyRec(curnode, relyid) {
                var ar = tmpR.addeds;
                //单路向上清理依赖项记录
                while (curnode) {
                    var n = curnode.attributes.parent;
                    for (var x = 0; x < ar.length; x++) {
                        var item = ar[x];
                        if (n == item.reliedNode && relyid == item.relyItem) {
                            ar.splice(x, 1); x--;
                            break;
                        }
                    }
                    curnode = $('#menu_nr').tree('getParent', curnode.target);
                }
            };
            //再向下清理子级的依赖项记录
            curnode = node;
            clearChildRelyRec(curnode);
            function clearChildRelyRec(curnode) {
                //先清理本级节点
                var ca = curnode.attributes;
                var id = ca.dataid;
                if (ca.relyonnode) {
                    if (ca.relyonnode != "") clearRelyRec(curnode, id);
                }
                //再清理子级节点
                var childs = curnode.children;
                if (childs && childs.length > 0) {
                    for (var i = 0; i < childs.length; i++) {
                        var c = childs[i], ca = c.attributes;
                        var id = ca.dataid;
                        if (ca.relyonnode) {
                            if (ca.relyonnode != "") clearRelyRec(c, id);
                        }
                        if (c.children) {
                            clearChildRelyRec(c);
                        }
                    }
                }
            }
        }
        $('#menu_nr').tree('remove', node.target);
        var datsection = $("a[name='" + node.id + "']");
        var istab = ($('#p_' + node.id)[0].hasAttribute('tabPart') ? true : false), tabtype = (istab ? $('#p_' + node.id).attr('tabPart') : ""), tabIndex = (istab ? Number($('#p_' + node.id).attr('tabIndex')) : -1);
        if (datsection) {
            if (istab) {
                //if (tabtype.toLowerCase() == "tabframe") {
                //    $('#tab_' + node.id).remove();
                //} else {
                var paranode = $('#menu_nr').tree('find', node.attributes.parent);
                if (document.getElementById('tab_' + paranode.id) == undefined) {
                    $('#tab_' + node.id).remove();
                } else {
                    $Tabs.removeTab('tab_' + paranode.id, tabIndex);
                }
                //}
            } else {
                while (datsection = datsection.parent()) {
                    if (datsection.length > 0 && datsection[0].tagName.toLowerCase() == 'div' && datsection[0].hasAttribute('policycontainer')) break;
                }
                datsection.remove();
            }
        }
    }
}
function collapse(node) {
    if (node) {
        $('#menu_nr').tree('collapse', node.target);
        //alert($('#p_' + node.id).panel('options').collapsed);
        if ($('#p_' + node.id).hasClass('panel')) $('#p_' + node.id).panel('collapse', true);
    }
}
function expand(node) {
    if (node) {
        $('#menu_nr').tree('expand', node.target);
        //alert($('#p_' + node.id).panel('options').collapsed);
        if ($('#p_' + node.id).hasClass('panel')) $('#p_' + node.id).panel('expand', true);
    }
}

function setCheckbox(value, row, index) {
    return $("<input type='check'/>") + row.value;
}
