# 易览资讯数据

数据库：MySQL
192.168.37.102
ylzx

用户关注标签信息：`YLZX_NRGL_OPER_CATEGORY`表中的 `OPERATOR_ID`列和`CATEGORY_NAME`列，

用户订阅网站信息：`YLZX_NRGL_MYSUB_WEBSITE`

用户订阅网站栏目信息：`YLZX_NRGL_MYSUB_WEBSITE_COL` 表中的`OPERATOR_ID`和`COLUMN_ID`列

用户信息表：`AC_OPERATOR`
    OPERATOR_ID：用户ID
    START_DATE：注册时间
    LAST_LOGIN：最近登陆时间

手机app登陆情况：`SYS_APP_USERINFO`
    LOGINTIME：登陆时间
    IMEI：安卓手机的唯一标识


数据库HBASE

t_hbaseSink

info:logID //LOG_ID 编号
info:tYPE //TYPE 1：接入日志；2：错误日志'
info:cREATE_BY //CREATE_BY 创建者
info:cREATE_BY_ID //CREATE_BY_ID 登录人ID
info:cREATE_TIME //CREATE_TIME 创建时间
info:rEMOTE_ADDR //REMOTE_ADDR 操作IP地址
info:uSER_AGENT //USER_AGENT 用户代理
info:rEQUEST_URI //REQUEST_URI 请求URI
info:mETHOD //METHOD 操作方式
info:pARAMS//PARAMS 操作提交的数据

iOS系统手机访问记录：
    rEQUEST_URI：/app/login/v1/updatePushIdByToken.do
    pARAMS：pushId=

安卓系统手机访问记录：
    rEQUEST_URI：/app/init/v1.do
    pARAMS：IMEI=

用户行为：
    rEQUEST_URI
        app：
        addFavorite.do 收藏
        delFavorite.do 取消收藏
        addComment.do 评论
        getContentById.do 浏览
        web：
        getContentById.do 浏览
        favorite/add.do 收藏
        favorite/delete.do 取消收藏
        like/add.do 点赞
            使用以下几种：
            getContentById.do 浏览
            favorite/add.do 收藏
            addFavorite.do 收藏
            favorite/delete.do 取消收藏
            delFavorite.do 取消收藏
            like/add.do 点赞
            addComment.do 评论(暂时不使用)
    pARAMS: id=