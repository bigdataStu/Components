# 电影数据说明

电影数据请参考于 [电影数据](moive-data)

```
 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
 数据描述：
 1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 3，"movies.dat"：MovieID::Title::Genres
 4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构 Haskset 的方式存在，是为了做 mapjoin
```

### 需求一

1. 读取信息,统计数据条数.   职业数, 电影数, 用户数, 评分条数
1. 显示 每个职业 下的用户详细信息    显示为:  (   职业编号,( 人的编号,性别,年龄,邮编 ), 职业名)

### 需求二

1. 某个用户看过的电影数量
1. 这些电影的信息，格式为:  (MovieId,Title,Genres)

### 需求三

1. 某个用户看过的电影数量
1. 这些电影的信息，格式为:  (MovieId,Title,Genres)

### 需求四

1. 所有电影中平均得分最高的前 10 部电影:    ( 分数,电影 ID)
1. 观看人数最多的前 10 部电影:   (观影人数,电影ID)


### 需求五

1. 分析男性用户最喜欢看的前 10 部电影
1. 女性用户最喜欢看的前 10 部电影  输出格式:  ( movieId, 电影名,总分，打分次数，平均分)
1. 分析最受不同年龄段人员欢迎的电影的前10 

原始数据集中的年龄段划分

```
* under 18: 1
* 18 - 24: 18
* 25 - 34: 25
* 35 - 44: 35
* 45 - 49: 45
* 50 - 55: 50
* 56 - + : 56
```

### 需求六

1. 分析不同类型的电影总数  输出格式: ( 类型,数量)


### 需求七

1. 分析每年度生产的电影总数  输出格式: (年度,数量)

### 需求八

1. 分析每年度不同类型的电影生产总数

### 需求九

1. 分析不同职业对观看电影类型的影响 格式: (职业名,(电影类型,观影次数))

### 需求十

1. 需求九结果入库 格式:  (职业名,(电影类型,观影次数))