# ylzx_定时任务

时间：2017年04月01日
服务器：slave4
事件：启动ylzx_xgwz.sh、ylzx_cnxh.sh和ylzx_hotLabel.sh定时任务
      ylzx_xgwz.sh：5点执行
	  ylzx_cnxh.sh：5点30分执行
	  ylzx_hotLabels.sh：6点执行

参考链接：
http://blog.csdn.net/blank1990/article/details/50457380

1.进入root账户
su root
2查看crontab服务状态
service crond status

启动服务
service crond start

3.新建定时任务
crontab -u root -e

30 5,12,15,18 * * * /bin/sh /root/lulu/Workspace/spark/yeeso/RecommendSys/shScript/ylzx_xgwz.sh

0 6,12,15,18 * * * /bin/sh /root/lulu/Workspace/spark/yeeso/RecommendSys/shScript/ylzx_cnxh.sh

30 6,13,16,19 * * * /bin/sh /root/lulu/Workspace/spark/yeeso/RecommendSys/shScript/ylzx_hotLabels.sh

由于计算文章相似性所需时间较长，定时任务后修改为：
30 18 * * * /bin/sh /root/lulu/Workspace/spark/yeeso/RecommendSys/shScript/ylzx_xgwz.sh

0 6,12,15,18 * * * /bin/sh /root/lulu/Workspace/spark/yeeso/RecommendSys/shScript/ylzx_cnxh.sh

30 6,13,16,19 * * * /bin/sh /root/lulu/Workspace/spark/yeeso/RecommendSys/shScript/ylzx_hotLabels.sh

0 0,12 * * * /bin/sh /root/lulu/Workspace/spark/yeeso/RecommendSys/shScript/swt_recom.sh

0 7 * * * /bin/sh /root/lulu/Workspace/spark/yeeso/RecommendSys/shScript/ylzx_appRecom.sh

添加用户行为定时任务－2017年09月11日

20 4 * * * /bin/sh /root/lulu/Workspace/spark/yeeso/RecommendSys/shScript/ylzx_tj_yhxw.sh

删除slave4上ylzx_xgwz.sh定时任务－2017年09月22日

在slave6上添加定时任务

0 8 * * * /bin/sh /root/lulu/Progect/ylzx/ylzx_xgwz.sh

0 22 * * * /bin/sh /root/lulu/Progect/ylzx/ylzx_cnxh.sh

0 3 * * * /bin/sh /root/lulu/Progect/ylzx/ylzx_tj_mt_yhxw.sh

将slave6上的定时任务迁移到bigdata7上（时间：2017年10月17日）

0 8 * * * /bin/sh /root/lulu/Progect/ylzx/ylzx_xgwz.sh

0 22 * * * /bin/sh /root/lulu/Progect/ylzx/ylzx_cnxh.sh

0 3 * * * /bin/sh /root/lulu/Progect/ylzx/ylzx_tj_mt_yhxw.sh


将bigdata7上的定时任务迁移回到slave6上（时间：2017年11月14日）

0 8 * * * /bin/sh /root/lulu/Progect/ylzx/ylzx_xgwz.sh

0 22 * * * /bin/sh /root/lulu/Progect/ylzx/ylzx_cnxh.sh

0 3 * * * /bin/sh /root/lulu/Progect/ylzx/ylzx_tj_mt_yhxw.sh



将slave6上的定时任务迁移到bigdata7上（时间：2017年11月16日）

0 8 * * * /bin/sh /root/lulu/Progect/ylzx/ylzx_xgwz.sh

0 22 * * * /bin/sh /root/lulu/Progect/ylzx/ylzx_cnxh.sh

0 3 * * * /bin/sh /root/lulu/Progect/ylzx/ylzx_tj_mt_yhxw.sh

4.重新启动服务
service crond restart

5.查看进度
crontab -l

6.停掉该任务，输入下面命令，然后删除所执行的命令
crontab -u root -e



注意：
shell脚本中一定要添加：
source /root/.bashrc


#!/bin/bash
#name=$1

set -x
source /root/.bashrc
cd /root/software/spark-2.0.2/bin

spark-submit \
--class com.ecloud.Inglory.appRecom.appRecomV1 \
--master yarn \
--num-executors 2 \
--executor-cores 2 \
--executor-memory 2g \
--jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
/root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar \
yilan-total_webpage t_hbaseSink
