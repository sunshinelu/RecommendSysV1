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





        
时间：2017年08月13日

修改文章相似性计算代码：

     spark-submit \
     --class com.ecloud.Inglory.word2Vec.docSimilarityV2 \
     --master yarn --num-executors 4 \
     --executor-cores 4 \
     --executor-memory 4g \
     --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar /root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar \
     yilan-total_webpage t_userProfileV1 
 
任务运行时间在2小时以上，且任务出错。     

     spark-submit \
     --class com.ecloud.Inglory.word2Vec.docSimilarityV2 \
     --master yarn --num-executors 6 \
     --executor-cores 6 \
     --executor-memory 6g \
     --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar /root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar \
     yilan-total_webpage t_userProfileV1 
     

查看HBASE表中数据
     
     count 't_userProfileV1'
     count 'ylzx_xgwz'
     
生成word2Vec Model

     spark-submit \
     --class com.ecloud.Inglory.word2Vec.BuildWord2VecModel \
     --master yarn \
     --num-executors 2 \
     --executor-cores 2 \
     --executor-memory 2g \
     --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
     /root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar \
     yilan-total_webpage


        
时间：2017年08月15日

使用ml.feature.Word2Vec方法构建Word2Vec Model，并将模型保存到HDFS中

     spark-submit \
     --class com.ecloud.Inglory.word2Vec.BuildWord2VecModelDF \
     --master yarn \
     --num-executors 4 \
     --executor-cores 4 \
     --executor-memory 2g \
     --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
     /root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar \
     yilan-total_webpage
     
         
时间：2017年08月18日
    
     [root@slave4 shScript]# nohup ./ylzx_logsToHbase.sh &
     [1] 6872
     
     application_1503041014194_0001	
     
     
     count 't_hbaseSink'
     => 987491
     
     count 't_hbaseSink'
     => 987596

时间：2017年08月27日

     count 'ylzx_xgwz'
     => 415207

    count 'yilan-total_webpage'
     => 145611

     count 't_hbaseSink'
     => 1048736
     
时间：2017年08月28日
     
21:58
     
      count 'ylzx_xgwz'
      => 420745

时间：2017年08月29日

    count 'ylzx_xgwz_2'
    => 550925

时间：2017年08月31日

21:18

      count 'ylzx_xgwz'
      => 460312
      
时间：2017年09月04日

08:46

      count 'ylzx_xgwz'
      => 483539
      
      count 't_hbaseSink'
      => 1116495
  
任务莫名被清空，重新提交ylzx_logsToHbase任务。

[root@slave4 shScript]# nohup ./ylzx_logsToHbase.sh &
[1] 15768
[root@slave4 shScript]# nohup: ignoring input and appending output to `nohup.out'

application_1504508652498_0001

count 't_hbaseSink'
=> 1120028

count 't_hbaseSink'
=> 1120094

count 't_hbaseSink'
=> 1120340

时间：2017年09月05日

    count 'ylzx_xgwz'
    => 494187
    
时间：2017年09月08日

count 't_hbaseSink'
=> 1579627

日志规则

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

时间：2017年09月11日

count 't_hbaseSink'
=> 1679131

=> 1679131

=> 1679131

=> 1679131

t_hbaseSink表中的数据没有增加，数据存入的最新时间刚好与易览资讯部署时间相一致。
 使用 yarn application -list查看改任务，任务仍在运行，

重新提交ylzx_logsToHbase任务。

nohup ./ylzx_logsToHbase.sh > logsToHbase.out 2>&1 &

[1] 25053
application_1504508652498_0556

count 't_hbaseSink'
=> 1679174

=> 1679291

=> 1679457

=> 1679506

=> 1681229 （16:37）


时间：2017年09月12日

count 't_hbaseSink'

=> 1685981

=> 1686516


count 'yilan-total_webpage'

=> 173957

时间：2017年09月13日

count 'ylzx_xgwz'

=> 565501

时间：2017年09月14日

count 'yilan-total_webpage'
=> 178535

count 't_hbaseSink'

=> 1704832

=> 1705020

=> 1706065

count 'ylzx_xgwz'

=> 574941

时间：2017年09月20日

count 't_hbaseSink'

=> 1746504

=> 1746594


日志规则：

热门标签的点击情况：（REQUEST_URI    PARAMS）
search.do    manuallabel=政务, sType=index}	

搜索框检索情况：（REQUEST_URI    PARAMS）
searchByKeyword.do    {cuPage=1, keyword=大数据}

栏目点击情况：（REQUEST_URI    PARAMS）
search.do    {sType=index, xzqhname=中国}

时间：2017年09月22日

count 't_hbaseSink'

=> 3252149

count 'docsimi_title'

=> 622711

count 'ylzx_xgwz'

=> 607272

时间：2017年09月25日

count 'yilan-total_webpage'

=> 190958

count 't_hbaseSink'

=> 3286720

=> 3286764

count 'yilan-total-analysis_webpage'

时间：2017年09月28日

count 't_hbaseSink'

=> 3384424

=> 3384522

count 'yilan-total-analysis_webpage'

=> 192177

时间：2017年09月30日

slave6上vi ylzx_xgwz.sh


#!/bin/bash
#name=$1

set -x
source /root/.bashrc
cd /root/software/spark-2.1.0-bin-hadoop2.6/bin

spark-submit \
--class com.ecloud.Inglory.DocsSimilarity.DocsimiTitleV2 \
--master yarn \
--num-executors 8 \
--executor-cores 4 \
--executor-memory 6g \
--conf spark.default.parallelism=150 \
--conf spark.storage.memoryFraction=0.5 \
--conf spark.shuffle.memoryFraction=0.4 \
--jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
/root/lulu/Progect/ylzx/RecommendSysV1.jar \
yilan-total-analysis_webpage ylzx_xgwz

使用Jaccard Distance计算近一月数据与近一年数据之间的相似性。

count 'ylzx_xgwz'

=> 888166

=> 906207



时间：2017年10月09日

count 't_hbaseSink'

=> 4111385

=> 4111442

count 'ylzx_xgwz'

=> 925614

=> 926490

count 'yilan-total-analysis_webpage'

=> 199212


时间：2017年10月10日

count 'ylzx_xgwz'

=> 926821

=> 933493


时间：2017年10月11日

count 'ylzx_xgwz'

=> 939308

=> 939468

spark-submit \
--class com.ecloud.Inglory.DocsSimilarity.DocsimiTitleV4 \
--master yarn \
--deploy-mode client \
--num-executors 4 \
--executor-cores 4 \
--executor-memory 4g \
/root/lulu/Progect/ylzx/RecommendSysV1-1.0-SNAPSHOT-jar-with-dependencies.jar \
yilan-total-analysis_webpage ylzx_xgwz ylzx_xgwz_temp

count 'ylzx_xgwz'

=> 939493

=> 939498

---------------------
需求：
 
1、对所有用户当天/前天阅读的文章分别进行关键词提取，根据词性进行过滤，只保留名词。
每篇文章至多保存5个关键词，然后对关键词进行wordcount，取top 50～100.

2、对所有用户当天/前天阅读的文章所在的行政区划（p:xzqhname）进行wordcount。


最终结果为：
主键、时间（年月日）、关键词（词：权重；词：权重）、区域（行政编号a：权重；行政编号b：权重）、创建时间（年月日时分秒）


待办事项：
分词词库（统一维护）
停用词词库（统一维护）

-----------------

时间：2017年10月13日

count 't_hbaseSink'

=> 4131423

=> 4131690


时间：2017年10月16日

读取易览资讯数据时增加过滤条件

p:sfzs != 0 （是否展示，1表示展示，0表示不展示）
p:sfcj != 1 （是否采集，1表示重复采集，0表示未重复采集）

时间：2017年10月23日

在DocsimiTitleV5中添加：

MyStaticValue.userLibrary = "/root/lulu/Progect/NLP/userDic_20171023.txt"// bigdata7路径

字典数量为：11906305个

时间：2017年10月16日

将userDic_20171023.txt中的拼音替换成空，空格替换成空。

userDic_20171024.txt中字典数量为：11424791个


count 't_hbaseSink'

=> 4202899
=> 4203007

时间：2017年10月27

任务莫名被清空，重新提交ylzx_logsToHbase任务。

[root@slave4 shScript]# nohup ./ylzx_logsToHbase.sh > ylzx_logsToHbase.out 2>&1 &
[1] 28899

application_1509067561924_0001	

count 't_hbaseSink'

=> 4254739

=> 4254811

时间：2017年11月16

任务莫名被清空，重新提交ylzx_logsToHbase任务。

[root@slave4 ~]# cd lulu/Workspace/spark/yeeso/RecommendSys/shScript/
[root@slave4 shScript]# nohup ./ylzx_logsToHbase.sh > ylzx_logsToHbase.out 2>&1 &
[1] 11540

application_1510795685747_0002

count 't_hbaseSink'

=> 4314609

=> 4314675

=> 4314805

时间：2017年11月17

count 't_hbaseSink'

=> 4316927


时间：2017年11月30

count 't_hbaseSink'

=> 4339284

=> 4339441

时间：2017年12月04

count 't_hbaseSink'

=> 4437017

count 'ylzx_xgwz'

=> 1305490

时间：2018年01月15

count 't_hbaseSink'

=> 4540933


count 'ylzx_xgwz'

=> 1592253

时间：2018年01月22

count 'yilan-total-analysis_webpage'

=> 409036


时间：2018年02月10

cd lulu/Workspace/spark/yeeso/RecommendSys/shScript/
nohup ./ylzx_logsToHbase.sh > ylzx_logsToHbase_20180210.out 2>&1 &

[root@slave4 ~]# cd lulu/Workspace/spark/yeeso/RecommendSys/shScript/
[root@slave4 shScript]# nohup ./ylzx_logsToHbase.sh > ylzx_logsToHbase_20180210.out 2>&1 &
[1] 24912

count 't_hbaseSink'

=> 4572504

时间：2018年02月27


cd lulu/Workspace/spark/yeeso/RecommendSys/shScript/
nohup ./ylzx_logsToHbase.sh > ylzx_logsToHbase_20180227.out 2>&1 &


[root@slave4 ~]# cd lulu/Workspace/spark/yeeso/RecommendSys/shScript/
[root@slave4 shScript]# nohup ./ylzx_logsToHbase.sh > ylzx_logsToHbase_20180227.out 2>&1 &
[1] 23710

时间：2018年04月24

cd lulu/Workspace/spark/yeeso/RecommendSys/shScript/
nohup ./ylzx_logsToHbase.sh > ylzx_logsToHbase_20180424.out 2>&1 &

[root@slave4 ~]# cd lulu/Workspace/spark/yeeso/RecommendSys/shScript/
[root@slave4 shScript]# nohup ./ylzx_logsToHbase.sh > ylzx_logsToHbase_20180424.out 2>&1 &
[1] 16811

hbase shell

count 't_hbaseSink'

=> 4656485

=> 4656547


时间：2018年06月16

链接slave4，输入以下命令：

cd lulu/Workspace/spark/yeeso/RecommendSys/shScript/
nohup ./ylzx_logsToHbase.sh > ylzx_logsToHbase_20180616.out 2>&1 &

hbase shell

count 't_hbaseSink'

报错
ERROR: org.apache.hadoop.hbase.NotServingRegionException: Region t_hbaseSink,8fa72b43-a7ef-4155-876c-4643b1c66302,1510716904164.ca9cf3274c592f051a2b99a67f0fe788. is not online on bigdata2.yiyun,60020,1528941187639
	at org.apache.hadoop.hbase.regionserver.HRegionServer.getRegionByEncodedName(HRegionServer.java:2774)
	at org.apache.hadoop.hbase.regionserver.HRegionServer.getRegion(HRegionServer.java:4257)
	at org.apache.hadoop.hbase.regionserver.HRegionServer.scan(HRegionServer.java:3156)
	at org.apache.hadoop.hbase.protobuf.generated.ClientProtos$ClientService$2.callBlockingMethod(ClientProtos.java:29994)
	at org.apache.hadoop.hbase.ipc.RpcServer.call(RpcServer.java:2078)
	at org.apache.hadoop.hbase.ipc.CallRunner.run(CallRunner.java:108)
	at org.apache.hadoop.hbase.ipc.RpcExecutor.consumerLoop(RpcExecutor.java:114)
	at org.apache.hadoop.hbase.ipc.RpcExecutor$1.run(RpcExecutor.java:94)
	at java.lang.Thread.run(Thread.java:745)

时间：2018年07月02日

disable 't_hbaseSink'
drop 't_hbaseSink' # drop命令失败 
creat 't_hbaseSink','info'

count 't_hbaseSink'

enable 't_hbaseSink'


create 'ylzx_logs_20180702','info'
count 'ylzx_logs_20180702'

cd lulu/Workspace/spark/yeeso/RecommendSys/shScript/
nohup ./ylzx_logsToHbase.sh > ylzx_logsToHbase_20180702.out 2>&1 &


application_1524475208781_3992	com.ecloud.Inglory.Streaming.logsToHbase	               SPARK	      root   default	           RUNNING	         UNDEFINED	            10%	          http://192.168.37.14:4040

［slave4］

crontab -l

30 6,13,16,19 * * * /bin/sh /root/lulu/Workspace/spark/yeeso/RecommendSys/shScript/ylzx_hotLabels.sh
0 12 * * * /bin/sh /root/lulu/Workspace/spark/yeeso/RecommendSys/shScript/swt_recom.sh
0 7 * * * /bin/sh /root/lulu/Workspace/spark/yeeso/RecommendSys/shScript/ylzx_appRecom.sh
20 4 * * * /bin/sh /root/lulu/Workspace/spark/yeeso/RecommendSys/shScript/ylzx_tj_yhxw.sh

vi ylzx_hotLabels.sh
执行命令修改：将t_hbaseSink改为ylzx_logs_20180702

vi ylzx_appRecom.sh
执行命令修改：将t_hbaseSink改为ylzx_logs_20180702

vi ylzx_tj_yhxw.sh
执行命令修改：将t_hbaseSink改为ylzx_logs_20180702


［bigdata7］
crontab -l
0 8 * * * /bin/sh /root/lulu/Progect/ylzx/ylzx_xgwz.sh
0 22 * * * /bin/sh /root/lulu/Progect/ylzx/ylzx_cnxh.sh
0 3 * * * /bin/sh /root/lulu/Progect/ylzx/ylzx_tj_mt_yhxw.sh
11 0 * * * /bin/sh /root/lulu/Progect/Test/ylzx_userProfile_retention.sh
30 1 * * * /bin/sh /root/lulu/Progect/Test/ylzx_userProfile_appDAU.sh


vi ylzx_cnxh.sh
执行命令修改：将t_hbaseSink改为ylzx_logs_20180702

vi ylzx_tj_mt_yhxw.sh
执行命令修改：将t_hbaseSink改为ylzx_logs_20180702

/root/lulu/Progect/Test/ylzx_userProfile_retention.sh
在代码中修改，在代码中将t_hbaseSink改为ylzx_logs_20180702

/root/lulu/Progect/Test/ylzx_userProfile_appDAU.sh
在代码中修改，在代码中将t_hbaseSink改为ylzx_logs_20180702

时间：2018年07月03

count 'yilan-total-analysis_webpage'

=> 628275

count 't_hbaseSink'
=> 4764007


count 'ylzx_logs_20180702'
=> 653