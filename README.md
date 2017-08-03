时间：2017年04月21日（测试成功）

    truncate 't_hbaseSink'

vi ylzx_logsToHbase.sh

    set -x
    source /root/.bashrc
    #scp -r 192.168.37.103:/usr/soft/ylzx/logs/log_* /root/lulu/Workspace/spark/yeeso/RecommendSys/logs/
    #hadoop fs -rmr /personal/sunlu/ylzx_app/*
    #hadoop fs -put /root/lulu/Workspace/spark/yeeso/RecommendSys/logs/log_* /personal/sunlu/ylzx_app/
    cd /root/software/spark-2.0.2/bin
    
    spark-submit \
    --class com.ecloud.Inglory.Streaming.logsToHbase \
    --master yarn \
    --num-executors 8 \
    --executor-cores 8 \
    --executor-memory 4g \
    --jars /root/lulu/Program/jarLibs/spark-streaming-kafka_2.11-1.6.3.jar,/root/lulu/Program/jarLibs/spark-streaming-flume_2.11-2.1.0.jar,/root/lulu/Program/jarLibs/kafka_2.11-0.10.0.1.jar,/root/lulu/Program/jarLibs/zkclient-0.8.jar,/root/lulu/Program/jarLibs/metrics-core-2.2.0.jar,/root/lulu/Program/jarLibs/metrics-annotation-2.2.0.jar,/root/lulu/Program/jarLibs/kafka-clients-0.10.0.1.jar \
    /root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar
         

    nohup ./ylzx_logsToHbase.sh &

    application_1489628831882_9936	root	com.ecloud.Inglory.Streaming.logsToHbase	SPARK	default	Fri Apr 21 17:23:48 +0800 2017	N/A	ACCEPTED	UNDEFINED	

    count 't_hbaseSink'

时间：2017年04月27日（测试成功）
    构建评分系统：RatingSysV2 －－ 添加衰减因子r和行为权重
    
    vi ylzx_cnxh.sh
    
    #!/bin/bash
    #name=$1
    
    set -x
    source /root/.bashrc
    #scp -r 192.168.37.103:/usr/soft/ylzx/logs/log_* /root/lulu/Workspace/spark/yeeso/RecommendSys/logs/
    #hadoop fs -rmr /personal/sunlu/ylzx_app/*
    #hadoop fs -put /root/lulu/Workspace/spark/yeeso/RecommendSys/logs/log_* /personal/sunlu/ylzx_app/
    cd /root/software/spark-2.0.2/bin
    
    spark-submit --class com.ecloud.Inglory.RatingSys.RatingSysV2 \
    --master yarn \
    --num-executors 8 \
    --executor-cores 8 \
    --executor-memory 4g \
    --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar,/root/lulu/Program/jarLibs/spark-streaming-kafka_2.11-1.6.3.jar,/root/lulu/Program/jarLibs/spark-streaming-flume_2.11-2.1.0.jar,/root/lulu/Program/jarLibs/kafka_2.11-0.10.0.1.jar,/root/lulu/Program/jarLibs/zkclient-0.8.jar,/root/lulu/Program/jarLibs/metrics-core-2.2.0.jar,/root/lulu/Program/jarLibs/metrics-annotation-2.2.0.jar,/root/lulu/Program/jarLibs/kafka-clients-0.10.0.1.jar \
    /root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar \
    yilan-total_webpage t_hbaseSink  ylzx_cnxh
    
时间：2017年04月27日（测试成功）
    添加RatingSysV3
    在RatingSysV2的基础上对时间进行过滤，删除20年前的数据。并在输出结果中保存当前系统时间。
    运行RatingSysV3代码如下：
    
        spark-submit --class com.ecloud.Inglory.RatingSys.RatingSysV3 \
        --master yarn \
        --num-executors 8 \
        --executor-cores 8 \
        --executor-memory 4g \
        --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar,/root/lulu/Program/jarLibs/spark-streaming-kafka_2.11-1.6.3.jar,/root/lulu/Program/jarLibs/spark-streaming-flume_2.11-2.1.0.jar,/root/lulu/Program/jarLibs/kafka_2.11-0.10.0.1.jar,/root/lulu/Program/jarLibs/zkclient-0.8.jar,/root/lulu/Program/jarLibs/metrics-core-2.2.0.jar,/root/lulu/Program/jarLibs/metrics-annotation-2.2.0.jar,/root/lulu/Program/jarLibs/kafka-clients-0.10.0.1.jar \
        /root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar \
        yilan-total_webpage t_hbaseSink  t_RatingSys
        
         count 't_RatingSys'
         
        count 't_hbaseSink'
        
    
        spark-submit --class com.ecloud.Inglory.RatingSys.RatingSysV3 \
        --master yarn \
        --num-executors 8 \
        --executor-cores 8 \
        --executor-memory 4g \
        --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar,/root/lulu/Program/jarLibs/spark-streaming-kafka_2.11-1.6.3.jar,/root/lulu/Program/jarLibs/spark-streaming-flume_2.11-2.1.0.jar,/root/lulu/Program/jarLibs/kafka_2.11-0.10.0.1.jar,/root/lulu/Program/jarLibs/zkclient-0.8.jar,/root/lulu/Program/jarLibs/metrics-core-2.2.0.jar,/root/lulu/Program/jarLibs/metrics-annotation-2.2.0.jar,/root/lulu/Program/jarLibs/kafka-clients-0.10.0.1.jar \
        /root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar \
        yilan-total_webpage t_hbaseSink  ylzx_cnxh
        
        count 'ylzx_cnxh'
        
        count 't_hbaseSink'
   修改ylzx_cnxh.sh文件
        vi ylzx_cnxh.sh
       
时间：2017年05月17日

正式环境
CREATE_BY_ID：
20cb992f-0947-4caa-961a-df9a60e60551
21feda84-f674-4368-a93c-ce27737895e5
2d2b6097-09f4-495b-a1b8-1ab2e1414472
5388f26d-d98a-4d57-981f-b0b641e99673
66652c47-e75a-4f82-886b-daa320c54a1a
6b18aacb-b2b8-4618-8abf-fec7e85005f1
91bd68b9-2cb0-4f2f-9ee1-cc2fa473829c
d5e13d39-4889-4c26-95ec-1d5fb4d8d495

测试环境：
CREATE_BY_ID：
20cb992f-0947-4caa-961a-df9a60e60551
21feda84-f674-4368-a93c-ce27737895e5
66652c47-e75a-4f82-886b-daa320c54a1a
6b18aacb-b2b8-4618-8abf-fec7e85005f1
91bd68b9-2cb0-4f2f-9ee1-cc2fa473829c
d5e13d39-4889-4c26-95ec-1d5fb4d8d495

deleteHbaseData2
        spark-submit --class com.ecloud.Inglory.hbaseOperation.deleteHbaseData2 \
        --master yarn \
        --num-executors 8 \
        --executor-cores 8 \
        --executor-memory 4g \
        /root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar
    
    count 't_hbaseSink'
    => 223286
    
    count 't_hbaseSink'
    => 221611

时间：2017年05月17日
RatingSysV4
 *  * 构建评分系统：
 * 衰减因子r、行为权重
 *
 * 在RatingSysV1的基础上，对浏览、点赞、收藏、取消收藏的数据赋予不同的打分。
 * 在RatingSysV2的基础上对时间进行过滤，删除20年前的数据。并在输出结果中保存当前系统时间
 * 在RatingSysV3的基础上对易览资讯数据内容进行过滤。（因为在推荐的结果中存在内容相同的数据）
 *
 * args(0):易览资讯数据所在HBASE表
 * args(1):日志所在HBASE表
 * args(2)：输出HBASE表

        spark-submit --class com.ecloud.Inglory.RatingSys.RatingSysV4 \
        --master yarn \
        --num-executors 8 \
        --executor-cores 8 \
        --executor-memory 4g \
        --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar,/root/lulu/Program/jarLibs/spark-streaming-kafka_2.11-1.6.3.jar,/root/lulu/Program/jarLibs/spark-streaming-flume_2.11-2.1.0.jar,/root/lulu/Program/jarLibs/kafka_2.11-0.10.0.1.jar,/root/lulu/Program/jarLibs/zkclient-0.8.jar,/root/lulu/Program/jarLibs/metrics-core-2.2.0.jar,/root/lulu/Program/jarLibs/metrics-annotation-2.2.0.jar,/root/lulu/Program/jarLibs/kafka-clients-0.10.0.1.jar \
        /root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar \
        yilan-total_webpage t_hbaseSink  ylzx_cnxh
        
        count 'ylzx_cnxh'
        
        count 't_hbaseSink'

修改ylzx_cnxh.sh文件
vi ylzx_cnxh.sh

    #!/bin/bash
    #name=$1
    
    set -x
    source /root/.bashrc
    cd /root/software/spark-2.0.2/bin
    
    spark-submit --class com.ecloud.Inglory.RatingSys.RatingSysV4 \
    --master yarn \
    --num-executors 8 \
    --executor-cores 8 \
    --executor-memory 4g \
    --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar,/root/lulu/Program/jarLibs/spark-streaming-kafka_2.11-1.6.3.jar,/root/lulu/Program/jarLibs/spark-streaming-flume_2.11-2.1.0.jar,/root/lulu/Program/jarLibs/kafka_2.11-0.10.0.1.jar,/root/lulu/Program/jarLibs/zkclient-0.8.jar,/root/lulu/Program/jarLibs/metrics-core-2.2.0.jar,/root/lulu/Program/jarLibs/metrics-annotation-2.2.0.jar,/root/lulu/Program/jarLibs/kafka-clients-0.10.0.1.jar \
    /root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar \
    yilan-total_webpage t_hbaseSink  ylzx_cnxh
运行成功！

时间：2017年05月20日
docSimilarity

 * 由于计算文章相似性的代码运行时间较长（超过4小时），现修改计算文章相似性计算策略。
 * 获取近5年的数据进行分析
 * 使用word2vec构建特征矩阵
 * 然后使用RDD的分布式矩阵计算相似性
 修改ylzx_xgwz.sh
 
 vi ylzx_xgwz.sh
 
    #!/bin/bash
    #name=$1
    
    set -x
    source /root/.bashrc
    cd /root/software/spark-2.0.2/bin
    
    spark-submit \
    --class com.ecloud.Inglory.word2Vec.docSimilarity \
    --master yarn \
    --num-executors 8 \
    --executor-cores 8 \
    --executor-memory 10g \
    --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
    /root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar \
    yilan-total_webpage ylzx_xgwz
    
运行成功！运行时间20多分钟。

    count 'ylzx_xgwz'
    => 107440	

    count 'ylzx_xgwz'
    => 141126

时间：2017年5月22日

    count 'ylzx_xgwz'
    => 146187
    
    count 'ylzx_xgwz'
    => 148038
    时间：2017年5月23日
    count 'ylzx_xgwz'
    => 151096

＝＝＝＝＝＝＝
    
    count 't_hbaseSink'
    => 231418

    yarn application -kill application_1489628831882_9936

vi ylzx_logsToHbase.sh

    set -x
    source /root/.bashrc
    #scp -r 192.168.37.103:/usr/soft/ylzx/logs/log_* /root/lulu/Workspace/spark/yeeso/RecommendSys/logs/
    #hadoop fs -rmr /personal/sunlu/ylzx_app/*
    #hadoop fs -put /root/lulu/Workspace/spark/yeeso/RecommendSys/logs/log_* /personal/sunlu/ylzx_app/
    cd /root/software/spark-2.0.2/bin
    
    spark-submit \
    --class com.ecloud.Inglory.Streaming.logsToHbase \
    --master yarn \
    --num-executors 4 \
    --executor-cores 4 \
    --executor-memory 4g \
    --jars /root/lulu/Program/jarLibs/spark-streaming-kafka_2.11-1.6.3.jar,/root/lulu/Program/jarLibs/spark-streaming-flume_2.11-2.1.0.jar,/root/lulu/Program/jarLibs/kafka_2.11-0.10.0.1.jar,/root/lulu/Program/jarLibs/zkclient-0.8.jar,/root/lulu/Program/jarLibs/metrics-core-2.2.0.jar,/root/lulu/Program/jarLibs/metrics-annotation-2.2.0.jar,/root/lulu/Program/jarLibs/kafka-clients-0.10.0.1.jar \
    /root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar
         

    nohup ./ylzx_logsToHbase.sh &

    application_1495424819406_0262	
    
    count 't_hbaseSink'
    => 231418
    count 't_hbaseSink'
    => 231458
    
 时间：2017年5月23日
 
     count 'ylzx_xgwz'(运行时间：07:39)
     => 151096   
     count 'ylzx_xgwz'(运行时间：09:22)
     => 152143
     
     count 'ylzx_xgwz'(运行时间：19:03)
     => 152695
     
     
application_1495424819406_0262任务意外被删，重新启动。
     
     nohup ./ylzx_logsToHbase.sh &
     
     application_1495507124102_0225
     
     count 't_hbaseSink'
     => 233037
     
     count 't_hbaseSink'
     => 233066
     
     
     spark-submit \
     --class com.ecloud.Inglory.LDA.docSimilarity \
     --master yarn \
     --num-executors 8 \
     --executor-cores 8 \
     --executor-memory 6g \
     --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
     /root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar \
     yilan-total_webpage t_userProfileV1	
     
     count 't_userProfileV1'
     
时间2017年05月24日


修改spark-defaults.conf文件，添加如下参数。

spark.shuffle.consolidateFiles=true
spark.speculation=true

修改上述参数以后，LDA.docSimilarity的运行时间由4个小时，缩减为2个小时。

     spark-submit \
     --class com.ecloud.Inglory.LDA.docSimilarity \
     --master yarn \
     --num-executors 8 \
     --executor-cores 8 \
     --executor-memory 6g \
     --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
     /root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar \
     yilan-total_webpage t_userProfileV1	
     
     count 't_userProfileV1'
     
     
     count 'ylzx_xgwz'(运行时间：19:03)
     => 154535
     
时间：2017年05月26日
     
     count 'ylzx_xgwz'(运行时间：07:55)
     => 156513
 

时间：2017年05月27日
     
     count 'ylzx_xgwz'(运行时间：07:48)
     => 158368
     
时间：2017年5月31日

    1、宋淑君：确定栏目（即一级标签），汇总后给孙露。
    2、孙露：根据word2vec和关键词提取方法，找出和栏目（即一级标签）相关的词，并将结果给宋淑君。
    3、宋淑君：根据孙露提供的每个一级标签对应的相关词，从中挑选出适合作为二级标签的词，并将结果汇总给孙露。
    4、孙露：根据宋淑君提供的一级标签VS二级标签对应表，给文章打上一级和二级标签。

确定一级标签
海洋 商务 政务 人才 科技

时间：2017年06月01日
     
     count 'ylzx_xgwz'(运行时间：14:23)
     => 162166
     
      ./ylzx_xgwz.sh
     count 'ylzx_xgwz'（运行时间：16:30）
     => 163840
     
时间：2017年06月02日     
 
     count 'ylzx_xgwz'（运行时间：09:06）
     => 163840
         
时间：2017年06月05日     
 
     count 'ylzx_xgwz'（运行时间：08:56）         
      => 164450

文章相似性计算代码报错

     spark-submit \
     --class com.ecloud.Inglory.LDA.docSimilarity \
     --master yarn --num-executors 8 \
     --executor-cores 8 \
     --executor-memory 6g \
     --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar /root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar \
     yilan-total_webpage t_userProfileV1 

修改word2Vec.docSimilarity代码
 
         val document = ds3.select("id","features").na.drop.rdd.map {
                case Row(id:Long, features: MLVector) =>  (id, Vectors.fromML(features))
         }
 修改为：
 
         val document = ds3.select("id","features").na.drop.rdd.map {
           case Row(id:Long, features: MLVector) =>  (id, Vectors.fromML(features))
         }.filter(_._2.size >= 2)
 
运行./ylzx_xgwz.sh，运行时间。

     count 'ylzx_xgwz'（运行时间：08:56）         
     => 164687
     
时间：2017年06月06日     
 
      
     count 'ylzx_xgwz'（运行时间：07:51)
     => 164910
      
时间：2017年06月06日         
com.ecloud.swt

     spark-submit \
     --class com.ecloud.swt.alsRecommend \
     --master yarn \
     --num-executors 2 \
     --executor-cores  2 \
     --executor-memory 2g \
     --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
     /root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar \
     yilan-total_webpage SPEC_LOG_CLICK

时间：2017年06月09日     
 
      
     count 'ylzx_xgwz'（运行时间：08:36)
     => 165201

     count 't_hbaseSink'
     => 269945
     => 270003
     
        
 时间：2017年06月13日     
  
       
      count 'ylzx_xgwz'（运行时间：09:38)       
       => 165381
       
       ./ylzx_xgwz
       count 'ylzx_xgwz'（运行时间：10:06) 
       => 170061
       
时间：2017年06月14日
       count 'ylzx_xgwz'（运行时间：10:01)
       => 170705
       
word2Vec.BuildWord2VecModel

对yilan-total_webpage数据构建word2Vec模型，找出与海洋、商务、政务、人才、科技最相关的500个词。

给文章打标签思路：
方法一：
  1、使用word2vec计算每一个标签词的相关词。
  2、使用LDA方法构建聚类模型。
  3、分析LDA模型。通过观察每一主题下的特征词，根据这些特征词将主题归属到某一个或者几个标签下。

方法二：
  1、1、使用word2vec计算每一个标签词的相关词，将这些词作为词典进行分词（后者分词后只保留这些词）。
  2、计算文章之间相似性，相似的文章有相似的标签。

       
时间：2017年06月14日
       
       count 'ylzx_xgwz'（运行时间：09:53)
       => 170906

     spark-submit \
     --class com.ecloud.Inglory.word2Vec.BuildWord2VecModel \
     --master yarn \
     --num-executors 2 \
     --executor-cores 2 \
     --executor-memory 2g \
     --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
     /root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar \
     yilan-total_webpage 

运行时间：19mins, 53sec

时间：2017年06月14日
      
       count 'ylzx_xgwz'
       => 171159

    val document = ds3.select("id","features").na.drop.rdd.map {
      case Row(id:Long, features: MLVector) =>  (id, Vectors.fromML(features))
    }.filter(_._2.size >= 2)
    
修改为

    val document = ds3.select("id","features").na.drop.rdd.map {
      case Row(id:Long, features: MLVector) =>  (id, Vectors.fromML(features))
    }.filter(_._2.size >= 2).distinct

ylzx_xgwz.sh

    count 'ylzx_xgwz'
    => 171252
    
时间：2017年06月22日

app推荐设计方案：
通过对用户的搜索词、标签词、关键词进行打分，选出Top30～40的词，然后保存到mysql表中。

搜索词：t_hbaseSink表中REQUEST_URI列中包含searchByKeyword.do的列，对应的PARAMS中的keyword值
标签词：yilan-total_webpage表中的p:manuallabel列
关键词：对yilan-total_webpage表中的p:c列进行关键词提取

搜索词、标签词、关键词标准化处理

搜索词、标签词、关键词权重设置：
搜索词：0.5
标签词：0.3
关键词：0.2

关于时间衰减因子问题：参考RatingSysV4中对时间的处理方法

时间：2017年06月23日
com.ecloud.Inglory.appRecom.appRecomV1 步骤：
1、读取用户日志数据t_hbaseSink：获取用户浏览、点赞、收藏，其中浏览权重为0.2，点赞权重为0.3，收藏权重为0.5，取消收藏权重为－0.5。
                  时间衰减因子权重：3天以内：0.9
                                  3～7天：0.8
                                  7天～半月：0.7
                                  半月～一月：0.6
                                  1～6月：0.5
                                  6月～1年：0.4 
                                  大于一年：0.3
2、读取用户日志数据t_hbaseSink：获取用户搜索的关键词，衰减因子处理方案如上。
3、读取yilan-total_webpage表：获取文章的标签，每篇文章中的标签权重均为1。
4、读取yilan-total_webpage表：对每篇文章进行关键词提取，每篇文章提取的关键词个数为10，每个关键词的权重均为一。
5、对搜索词进行标准化处理：对第2步的搜索词进行分组、加和、标准化操作。
6、对标签词进行标准化处理：将第1步获取的用户数据与第3步的标签数据进行left join。然后分组、加和、标准化处理。
7、对关键词进行标准化处理：将第1步获取的用户数据与第4步的关键词数据进行left join。然后分组、加和、标准化处理。
8、数据合并，将第5、6、7步结果进行full join。
9、权重加和：搜索词权重0.5，标签词权重0.3，关键词权重0.2。
10、数据整形：分组、排序、top40、reduceByKey
11、结果保存到mysql数据库。


     spark-submit \
     --class com.ecloud.Inglory.appRecom.appRecomV1 \
     --master yarn \
     --num-executors 2 \
     --executor-cores 2 \
     --executor-memory 2g \
     --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
     /root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar \
     yilan-total_webpage t_hbaseSink
     
时间：2017年06月27日

在com.ecloud.Inglory.appRecom.appRecomV1中添加功能：删除一天前数据功能



时间：2017年06月23日

在com.ecloud.Inglory.appRecom.appRecomV1中添加功能：保存的结果中添加一行top40的词

     spark-submit \
     --class com.ecloud.Inglory.appRecom.appRecomV1 \
     --master yarn \
     --num-executors 2 \
     --executor-cores 2 \
     --executor-memory 2g \
     --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
     /root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar \
     yilan-total_webpage t_hbaseSink


时间：2017年07月12日

     count 't_hbaseSink'
     => 415751
     => 415797

时间：2017年07月13日

com.ecloud.Inglory.RecommendSys.hotLabelsV4
由于rowkey的规则发生变化，提取rowkey的正则也要反正变化
第178行：
//  val reg2 = """id=(\w+\.){2}\w+.*,""".r
//  val urlString = reg2.findFirstIn(x.PARAMS.toString).toString.replace("Some(id=", "").replace(",)", "")
改为：
val reg2 = """id=\S*,|id=\S*}""".r
val urlString = reg2.findFirstIn(x.PARAMS.toString).toString.replace("Some(id=", "").replace(",)", "").replace("})", "")


时间：2017年07月14日

        count 'ylzx_xgwz'
        => 198117
        
时间：2017年07月21日
        
        count 't_hbaseSink'
        => 478922

        
时间：2017年07年25日

        count 'ylzx_xgwz'
        => 198544
        
        count 'ylzx_xgwz'
        => 198569
        
 时间：2017年08月01日   
     
        count 't_hbaseSink'
        => 609482
        
时间：2017年08月03日

RatingSysV5：采用基于物品的相似性的方法进行推荐

    spark-submit --class com.ecloud.Inglory.RatingSys.RatingSysV5 \
    --master yarn \
    --num-executors 8 \
    --executor-cores 8 \
    --executor-memory 4g \
    --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar,/root/lulu/Program/jarLibs/spark-streaming-kafka_2.11-1.6.3.jar,/root/lulu/Program/jarLibs/spark-streaming-flume_2.11-2.1.0.jar,/root/lulu/Program/jarLibs/kafka_2.11-0.10.0.1.jar,/root/lulu/Program/jarLibs/zkclient-0.8.jar,/root/lulu/Program/jarLibs/metrics-core-2.2.0.jar,/root/lulu/Program/jarLibs/metrics-annotation-2.2.0.jar,/root/lulu/Program/jarLibs/kafka-clients-0.10.0.1.jar \
    /root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar \
    yilan-total_webpage t_hbaseSink  ylzx_cnxh
   
    
    vi ylzx_cnxh.sh 
    
#!/bin/bash
#name=$1

set -x
source /root/.bashrc
#scp -r 192.168.37.103:/usr/soft/ylzx/logs/log_* /root/lulu/Workspace/spark/yeeso/RecommendSys/logs/
#hadoop fs -rmr /personal/sunlu/ylzx_app/*
#hadoop fs -put /root/lulu/Workspace/spark/yeeso/RecommendSys/logs/log_* /personal/sunlu/ylzx_app/
cd /root/software/spark-2.0.2/bin

spark-submit --class com.ecloud.Inglory.RatingSys.RatingSysV5 \
--master yarn \
--num-executors 8 \
--executor-cores 8 \
--executor-memory 4g \
--jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar,/root/lulu/Program/jarLibs/spark-streaming-kafka_2.11-1.6.3.jar,/root/lulu/Program/jarLibs/spark-streaming-flume_2.11-2.1.0.jar,/root/lulu/Program/jarLibs/kafka_2.11-0.10.0.1.jar,/root/lulu/Program/jarLibs/zkclient-0.8.jar,/root/lulu/Program/jarLibs/metrics-core-2.2.0.jar,/root/lulu/Program/jarLibs/metrics-annotation-2.2.0.jar,/root/lulu/Program/jarLibs/kafka-clients-0.10.0.1.jar \
/root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar \
yilan-total_webpage t_hbaseSink  ylzx_cnxh