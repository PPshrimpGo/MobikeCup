############################################################################
########################Calculating_probability_test_bike_correct###########
############################################################################

#######程序需要的表#############
#test
#train_m_distance_count
#bikeid_probability_correct
################################
#######程序生成的表#############
#test_bike_probability
#final_probability_test_bike
#final_test_bike
################################
####################前期的处理程序###############################################
DROP TABLE
IF EXISTS `test_bike`;

CREATE TABLE `test_bike`(
	`sequenceid` INT(11)NOT NULL AUTO_INCREMENT,
	`orderid` INT(11)NOT NULL DEFAULT '0',
	`bikeid` INT(11)DEFAULT NULL,
	`starttime` datetime DEFAULT NULL,
	`geohashed_start_loc` VARCHAR(7)DEFAULT NULL,
	PRIMARY KEY(`sequenceid`)
)ENGINE = MyISAM AUTO_INCREMENT = 1 DEFAULT CHARSET = ascii;

#INSERT INTO 目标表 (字段1, 字段2, ...) SELECT 字段1, 字段2, ... FROM 来源表;(这里的话字段必须保持一致)
INSERT INTO `test_bike`(
	`orderid`,
	`bikeid`,
	`starttime`,
	`geohashed_start_loc`
)SELECT
	orderid,
	bikeid,
	starttime,
	geohashed_start_loc
FROM
	test
ORDER BY
	bikeid,
	starttime DESC;

######################################################################
DROP TABLE
IF EXISTS `test_bike2`;

CREATE TABLE `test_bike2`(
	`sequenceid` INT(11)NOT NULL AUTO_INCREMENT,
	`orderid` INT(11)NOT NULL DEFAULT '0',
	`bikeid` INT(11)DEFAULT NULL,
	`starttime` datetime DEFAULT NULL,
	`geohashed_start_loc` VARCHAR(7)DEFAULT NULL,
	PRIMARY KEY(`sequenceid`)
)ENGINE = MyISAM AUTO_INCREMENT = 2 DEFAULT CHARSET = ascii;

#INSERT INTO 目标表 (字段1, 字段2, ...) SELECT 字段1, 字段2, ... FROM 来源表;(这里的话字段必须保持一致)
INSERT INTO `test_bike2`(
	`orderid`,
	`bikeid`,
	`starttime`,
	`geohashed_start_loc`
)SELECT
	orderid,
	bikeid,
	starttime,
	geohashed_start_loc
FROM
	test
ORDER BY
	bikeid,
	starttime DESC;

######################################################################
DROP TABLE
IF EXISTS `test_bike_link`;

CREATE TABLE test_bike_link SELECT
	b1.sequenceid,
	b1.orderid,
	b1.bikeid,
	b1.starttime,
	b2.starttime AS starttime_forecast,
	(
		unix_timestamp(b2.starttime)- unix_timestamp(b1.starttime)
	)/ 3600 AS time_interval,
	b1.geohashed_start_loc,
	b2.geohashed_start_loc AS geohashed_end_loc_forecast,
	floor(
		(
			(
				abs(g1.lat - g2.lat)* 111700 + abs(g1.lng - g2.lng)* 85567
			)+ 50
		)/ 100
	)AS m_distance100
FROM
	test_bike AS b1
INNER JOIN test_bike2 AS b2 ON b1.sequenceid = b2.sequenceid
AND b1.bikeid = b2.bikeid
INNER JOIN geohash_latlng AS g1 ON g1.geohashed = b1.geohashed_start_loc
INNER JOIN geohash_latlng AS g2 ON g2.geohashed = b2.geohashed_start_loc;

######################因为增加了修正步骤，原步骤生成了pre版本的概率##############################################
DROP TABLE
IF EXISTS `test_bike_pre_probability`;

CREATE TABLE test_bike_pre_probability SELECT
	`test_bike_link`.`sequenceid` AS `sequenceid`,
	`test_bike_link`.`orderid` AS `orderid`,
	`test_bike_link`.`bikeid` AS `bikeid`,
	`test_bike_link`.`starttime` AS `starttime`,
	`test_bike_link`.`starttime_forecast` AS `starttime_forecast`,
	`test_bike_link`.`time_interval` AS `time_interval`,
	`test_bike_link`.`geohashed_start_loc` AS `geohashed_start_loc`,
	`test_bike_link`.`geohashed_end_loc_forecast` AS `geohashed_end_loc_forecast`,
	`test_bike_link`.`m_distance100` AS `m_distance100`,
	`train_m_distance_count`.`m_distance_percent` AS `m_distance_percent`,
	floor(
		`train_m_distance_count`.`m_distance_percent` / `test_bike_link`.`time_interval` * 0.47490271 * 1000 + 0.5
	)/ 1000 AS geohashed_end_pre_probability
FROM
	(
		`test_bike_link`
		JOIN `train_m_distance_count` ON(
			(
				`train_m_distance_count`.`m_distance100` = `test_bike_link`.`m_distance100`
			)
		)
	);

ALTER TABLE test_bike_pre_probability ADD PRIMARY KEY(`orderid`);

ALTER TABLE test_bike_pre_probability ADD INDEX geohashed_end_pre_probability(
	`geohashed_end_pre_probability`
);

#####################利用train集对算法进行修正，并将修正数据事先存入bikeid_probability_correct文件中###############################
#####################利用train的数据对test数据的概率进行修正#######################################################################
DROP TABLE
IF EXISTS `test_bike_probability_correct`;

CREATE TABLE test_bike_probability_correct SELECT
	`test_bike_pre_probability`.`sequenceid` AS `sequenceid`,
	`test_bike_pre_probability`.`orderid` AS `orderid`,
	`test_bike_pre_probability`.`bikeid` AS `bikeid`,
	`test_bike_pre_probability`.`starttime` AS `starttime`,
	`test_bike_pre_probability`.`starttime_forecast` AS `starttime_forecast`,
	`test_bike_pre_probability`.`time_interval` AS `time_interval`,
	`test_bike_pre_probability`.`geohashed_start_loc` AS `geohashed_start_loc`,
	`test_bike_pre_probability`.`geohashed_end_loc_forecast` AS `geohashed_end_loc_forecast`,
	`test_bike_pre_probability`.`m_distance100` AS `m_distance100`,
	`test_bike_pre_probability`.`m_distance_percent` AS `m_distance_percent`,
	`test_bike_pre_probability`.`geohashed_end_pre_probability` AS `geohashed_end_pre_probability`,
	`bikeid_probability_correct`.`geohashed_end_probability` AS geohashed_end_probability
FROM
	(
		`test_bike_pre_probability`
		JOIN `bikeid_probability_correct` ON(
			(
				`bikeid_probability_correct`.`geohashed_end_pre_probability` = `test_bike_pre_probability`.`geohashed_end_pre_probability`
			)
		)
	);

ALTER TABLE test_bike_probability_correct ADD PRIMARY KEY(`orderid`);

##############################################################################
DROP TABLE
IF EXISTS `final_probability_test_bike_correct`;

CREATE TABLE final_probability_test_bike_correct SELECT
	`test_bike_probability_correct`.`orderid` AS `orderid`,
	`test_bike_probability_correct`.`geohashed_end_loc_forecast` AS `geohashed_end_loc1`,
	`test_bike_probability_correct`.`geohashed_end_probability` AS `geohashed_end_probability1`
FROM
	test_bike_probability_correct;

##############################################################################
DROP TABLE
IF EXISTS `final_test_bike_correct`;

CREATE TABLE final_test_bike_correct SELECT
	test.orderid AS `orderid`,
	test_bike_probability_correct.geohashed_end_loc_forecast AS `geohashed_end_loc1`
FROM
	test
LEFT JOIN test_bike_probability_correct ON test_bike_probability_correct.orderid = test.orderid;

UPDATE final_test_bike_correct
SET geohashed_end_loc1 = 'wx4sqpb'
WHERE
	ISNULL(geohashed_end_loc1);

##############################################################################
DROP TABLE
IF EXISTS `test_bike_link`;

DROP TABLE
IF EXISTS `test_bike2`;

DROP TABLE
IF EXISTS `test_bike`;


############################################################################
########################Calculating_probability_teststart_trainend##########
############################################################################

#######程序需要的表#############
#train_complete
#train_m_distance_count
#test_complete
#test
################################
#######程序生成的表#############
#teststart_trainend_probability
#teststart_trainend_probability_backup
#final_probability_teststart_trainend
#final_teststart_trainend
#################################
####################生成train表中所有的目标点集及其数量################################################
DROP TABLE
IF EXISTS `train_complete_end_loc_count`;

CREATE TABLE train_complete_end_loc_count SELECT
	train_complete.orderid AS orderid,
	train_complete.end_lat AS end_lat,
	train_complete.end_lng AS end_lng,
	train_complete.geohashed_end_loc AS geohashed_end_loc,
	Count(
		train_complete.geohashed_end_loc
	)AS geohashed_end_loc_count
FROM
	`train_complete`
GROUP BY
	train_complete.geohashed_end_loc;

ALTER TABLE train_complete_end_loc_count ADD PRIMARY KEY(`orderid`);

ALTER TABLE train_complete_end_loc_count ADD INDEX end_lat(`end_lat`);

ALTER TABLE train_complete_end_loc_count ADD INDEX end_lng(`end_lng`);

ALTER TABLE train_complete_end_loc_count ADD INDEX geohashed_end_loc(`geohashed_end_loc`);

####################生成test表中所有的起点集及其数量################################################
DROP TABLE
IF EXISTS `test_complete_start_loc_count`;

CREATE TABLE test_complete_start_loc_count SELECT
	test_complete.orderid AS orderid,
	test_complete.start_lat AS start_lat,
	test_complete.start_lng AS start_lng,
	test_complete.geohashed_start_loc AS geohashed_start_loc,
	Count(
		test_complete.geohashed_start_loc
	)AS geohashed_start_loc_count
FROM
	`test_complete`
GROUP BY
	test_complete.geohashed_start_loc;

ALTER TABLE test_complete_start_loc_count ADD PRIMARY KEY(`orderid`);

ALTER TABLE test_complete_start_loc_count ADD INDEX end_lat(`start_lat`);

ALTER TABLE test_complete_start_loc_count ADD INDEX end_lng(`start_lng`);

ALTER TABLE test_complete_start_loc_count ADD INDEX geohashed_start_loc(`geohashed_start_loc`);

####################生成曼哈顿距离2000米内，从起点到某目标点的距离及到目标点测试集数量###############
####################本步比较费时间，大约要跑半个多小时###############################################
DROP TABLE
IF EXISTS `teststart_trainend_relation_in2000m`;

CREATE TABLE teststart_trainend_relation_in2000m SELECT
	t1.geohashed_start_loc,
	t2.geohashed_end_loc,
	floor(
		(
			(
				abs(t1.start_lat - t2.end_lat)* 111700 + abs(t1.start_lng - t2.end_lng)* 85567
			)+ 50
		)/ 100
	)AS m_distance,
	t2.geohashed_end_loc_count
FROM
	test_complete_start_loc_count AS t1,
	train_complete_end_loc_count AS t2
WHERE
	t2.end_lat <(t1.start_lat + 0.017905103)
AND t2.end_lat >(t1.start_lat - 0.017905103)
AND t2.end_lng <(t1.start_lng + 0.023373497)
AND t2.end_lng >(t1.start_lng - 0.023373497)
AND(
	abs(t1.start_lat - t2.end_lat)* 111700 + abs(t1.start_lng - t2.end_lng)* 85567
)< 2000;

ALTER TABLE teststart_trainend_relation_in2000m ADD PRIMARY KEY(
	`geohashed_start_loc`,
	`geohashed_end_loc`
);

ALTER TABLE teststart_trainend_relation_in2000m ADD INDEX m_distance(`m_distance`);

DROP TABLE train_complete_end_loc_count;

DROP TABLE test_complete_start_loc_count;

##########生成某目标点周边曼哈顿距离2000米内目标点的吸引力概率，为从起点做准备#####
####################train_m_distance_count是从出发点走X距离的概率##################
DROP TABLE
IF EXISTS `teststart_trainend_probability_groupstart`;

CREATE TABLE teststart_trainend_probability_groupstart SELECT
	teststart_trainend_relation_in2000m.geohashed_start_loc,
	sum(
		teststart_trainend_relation_in2000m.geohashed_end_loc_count
	)AS start_count
FROM
	teststart_trainend_relation_in2000m
GROUP BY
	teststart_trainend_relation_in2000m.geohashed_start_loc;

ALTER TABLE teststart_trainend_probability_groupstart ADD PRIMARY KEY(`geohashed_start_loc`);


######################################################################
DROP TABLE
IF EXISTS `teststart_trainend_pre_probability`;

CREATE TABLE teststart_trainend_pre_probability SELECT
	teststart_trainend_relation_in2000m.geohashed_start_loc,
	teststart_trainend_relation_in2000m.geohashed_end_loc,
	teststart_trainend_relation_in2000m.m_distance,
	teststart_trainend_relation_in2000m.geohashed_end_loc_count,
	teststart_trainend_relation_in2000m.geohashed_end_loc_count / teststart_trainend_probability_groupstart.start_count AS pre_probability
FROM
	teststart_trainend_relation_in2000m
INNER JOIN teststart_trainend_probability_groupstart ON teststart_trainend_relation_in2000m.geohashed_start_loc = teststart_trainend_probability_groupstart.geohashed_start_loc;

ALTER TABLE teststart_trainend_pre_probability ADD PRIMARY KEY(
	`geohashed_start_loc`,
	`geohashed_end_loc`
);

####################生成曼哈顿距离2000米内，从起点到某目标点的概率值###############
####################train_m_distance_count是从出发点走X距离的概率##################
DROP TABLE
IF EXISTS `teststart_trainend_probability`;

CREATE TABLE teststart_trainend_probability SELECT
	teststart_trainend_pre_probability.geohashed_start_loc,
	teststart_trainend_pre_probability.geohashed_end_loc,
	teststart_trainend_pre_probability.m_distance,
	teststart_trainend_pre_probability.geohashed_end_loc_count,
	teststart_trainend_pre_probability.pre_probability * train_m_distance_count.m_distance_percent * 2.216636776 AS probability
FROM
	teststart_trainend_pre_probability
INNER JOIN train_m_distance_count ON teststart_trainend_pre_probability.m_distance = train_m_distance_count.m_distance100
ORDER BY
	probability DESC;

ALTER TABLE teststart_trainend_probability ADD PRIMARY KEY(
	`geohashed_start_loc`,
	`geohashed_end_loc`
);

DROP TABLE teststart_trainend_probability_groupstart;

DROP TABLE teststart_trainend_pre_probability;

####################复制曼哈顿距离2000米内，从起点到某目标点的概率值表###############
DROP TABLE
IF EXISTS `teststart_trainend_probability_backup`;

CREATE TABLE teststart_trainend_probability_backup SELECT
	*
FROM
	teststart_trainend_probability;

ALTER TABLE teststart_trainend_probability_backup ADD PRIMARY KEY(
	`geohashed_start_loc`,
	`geohashed_end_loc`
);

######################读取每个起点到某目标点最高概率值##########
DROP TABLE
IF EXISTS `teststart_trainend_probability_top1`;

CREATE TABLE teststart_trainend_probability_top1 SELECT
	*
FROM
	teststart_trainend_probability
GROUP BY
	geohashed_start_loc;

ALTER TABLE teststart_trainend_probability_top1 ADD PRIMARY KEY(
	`geohashed_start_loc`,
	`geohashed_end_loc`
);

DELETE
FROM
	teststart_trainend_probability
WHERE
	(
		geohashed_start_loc,
		geohashed_end_loc
	)IN(
		SELECT
			geohashed_start_loc,
			geohashed_end_loc
		FROM
			teststart_trainend_probability_top1
	);

######################读取每个起点到某目标点次高概率值##########
DROP TABLE
IF EXISTS `teststart_trainend_probability_top2`;

CREATE TABLE teststart_trainend_probability_top2 SELECT
	*
FROM
	teststart_trainend_probability
GROUP BY
	geohashed_start_loc;

ALTER TABLE teststart_trainend_probability_top2 ADD PRIMARY KEY(
	`geohashed_start_loc`,
	`geohashed_end_loc`
);

DELETE
FROM
	teststart_trainend_probability
WHERE
	(
		geohashed_start_loc,
		geohashed_end_loc
	)IN(
		SELECT
			geohashed_start_loc,
			geohashed_end_loc
		FROM
			teststart_trainend_probability_top2
	);

######################读取每个起点到某目标点第三高概率值########
DROP TABLE
IF EXISTS `teststart_trainend_probability_top3`;

CREATE TABLE teststart_trainend_probability_top3 SELECT
	*
FROM
	teststart_trainend_probability
GROUP BY
	geohashed_start_loc;

ALTER TABLE teststart_trainend_probability_top3 ADD PRIMARY KEY(
	`geohashed_start_loc`,
	`geohashed_end_loc`
);

DELETE
FROM
	teststart_trainend_probability
WHERE
	(
		geohashed_start_loc,
		geohashed_end_loc
	)IN(
		SELECT
			geohashed_start_loc,
			geohashed_end_loc
		FROM
			teststart_trainend_probability_top3
	);

DROP TABLE teststart_trainend_probability;

######################生成预测结果，带概率值##########################
DROP TABLE
IF EXISTS `final_probability_teststart_trainend`;

CREATE TABLE final_probability_teststart_trainend SELECT
	`test`.`orderid` AS `orderid`,
	`teststart_trainend_probability_top1`.`geohashed_end_loc` AS `geohashed_end_loc1`,
	`teststart_trainend_probability_top1`.`probability` AS `geohashed_end_probability1`,
	`teststart_trainend_probability_top2`.`geohashed_end_loc` AS `geohashed_end_loc2`,
	`teststart_trainend_probability_top2`.`probability` AS `geohashed_end_probability2`,
	`teststart_trainend_probability_top3`.`geohashed_end_loc` AS `geohashed_end_loc3`,
	`teststart_trainend_probability_top3`.`probability` AS `geohashed_end_probability3`
FROM
	(
		(
			(
				`test`
				LEFT JOIN `teststart_trainend_probability_top1` ON(
					(
						`teststart_trainend_probability_top1`.`geohashed_start_loc` = `test`.`geohashed_start_loc`
					)
				)
			)
			LEFT JOIN `teststart_trainend_probability_top2` ON(
				(
					`teststart_trainend_probability_top2`.`geohashed_start_loc` = `test`.`geohashed_start_loc`
				)
			)
		)
		LEFT JOIN `teststart_trainend_probability_top3` ON(
			(
				`teststart_trainend_probability_top3`.`geohashed_start_loc` = `test`.`geohashed_start_loc`
			)
		)
	);

######################生成预测结果，缺失值随机填充wx4sqpb########
DROP TABLE
IF EXISTS `final_teststart_trainend`;

CREATE TABLE final_teststart_trainend SELECT
	`test`.`orderid` AS `orderid`,
	`teststart_trainend_probability_top1`.`geohashed_end_loc` AS `geohashed_end_loc1`,
	`teststart_trainend_probability_top2`.`geohashed_end_loc` AS `geohashed_end_loc2`,
	`teststart_trainend_probability_top3`.`geohashed_end_loc` AS `geohashed_end_loc3`
FROM
	(
		(
			(
				`test`
				LEFT JOIN `teststart_trainend_probability_top1` ON(
					(
						`teststart_trainend_probability_top1`.`geohashed_start_loc` = `test`.`geohashed_start_loc`
					)
				)
			)
			LEFT JOIN `teststart_trainend_probability_top2` ON(
				(
					`teststart_trainend_probability_top2`.`geohashed_start_loc` = `test`.`geohashed_start_loc`
				)
			)
		)
		LEFT JOIN `teststart_trainend_probability_top3` ON(
			(
				`teststart_trainend_probability_top3`.`geohashed_start_loc` = `test`.`geohashed_start_loc`
			)
		)
	);

UPDATE final_teststart_trainend
SET geohashed_end_loc1 = 'wx4sqpb'
WHERE
	ISNULL(geohashed_end_loc1);

UPDATE final_teststart_trainend
SET geohashed_end_loc2 = 'wx4sqpb'
WHERE
	ISNULL(geohashed_end_loc2);

UPDATE final_teststart_trainend
SET geohashed_end_loc3 = 'wx4sqpb'
WHERE
	ISNULL(geohashed_end_loc3);


DROP TABLE teststart_trainend_probability_top1;
DROP TABLE teststart_trainend_probability_top2;
DROP TABLE teststart_trainend_probability_top3;

###############################################################
#DROP TABLE;
#DROP TABLE;
###############################################################
#参考语句
#ALTER TABLE `table_name` ADD PRIMARY KEY ( `column` )
#ALTER TABLE `table_name` ADD UNIQUE (`column`)
#ALTER TABLE `table_name` ADD INDEX index_name ( `column` )



###############################################################################################
########################Calculating_probability_trainstart_trainend_nearby_cross_pro###########
###############################################################################################

#######程序需要的表#############
#train_complete
#test
#
################################
#######程序生成的表#############
#trainstart_trainend_probability_back_cross_pro
#final_probability_trainstart_trainend_nearby_cross_pro
#final_trainstart_trainend_nearby_cross_pro
################################
####################前期的处理程序###############################################
DROP TABLE
IF EXISTS `trainstart_trainend_count1`;

CREATE TABLE trainstart_trainend_count1 SELECT
	geohashed_start_loc,
	geohashed_end_loc,
	start_lat,
	start_lng,
	end_lat,
	end_lng,
	count(*)AS startend_count1
FROM
	train_complete
GROUP BY
	geohashed_start_loc,
	geohashed_end_loc
ORDER BY
	startend_count1 DESC;

ALTER TABLE trainstart_trainend_count1 ADD PRIMARY KEY(
	`geohashed_start_loc`,
	`geohashed_end_loc`
);

ALTER TABLE `trainstart_trainend_count1` ADD INDEX start_lat(`start_lat`);

ALTER TABLE `trainstart_trainend_count1` ADD INDEX start_lng(`start_lng`);

ALTER TABLE `trainstart_trainend_count1` ADD INDEX end_lat(`end_lat`);

ALTER TABLE `trainstart_trainend_count1` ADD INDEX end_lng(`end_lng`);

####################################3X3卷积核#################################################
################################geohash_latlng_nearby表作为基本表保留#########################
#CREATE TABLE geohash_latlng_nearby SELECT
#	t1.geohashed,
#	t2.geohashed AS geohashed_nearby
#FROM
#	geohash_latlng AS t1,
#	geohash_latlng AS t2
#WHERE
#	t2.lat <(t1.lat + 0.00138)
#AND t2.lat >(t1.lat - 0.00138)
#AND t2.lng <(t1.lng + 0.00138)
#AND t2.lng >(t1.lng - 0.00138);
#每个单元格的长与款都是0.0006866455078125*2=0.001373291015625，为了计算一定在其内，本段内使用0.00138
#####################################################################################################
################################3X3十字卷积核改进版，本身出发点权重乘2###########################
################################geohash_latlng_nearby表不作为基本表保留#########################
DROP TABLE
IF EXISTS `geohash_latlng_nearby`;

CREATE TABLE geohash_latlng_nearby SELECT
	t1.geohashed,
	t2.geohashed AS geohashed_nearby
FROM
	geohash_latlng AS t1,
	geohash_latlng AS t2
WHERE
   t2.lat = t1.lat AND t2.lng < (t1.lng + 0.0013733 + 0.0001) AND (t2.lng > t1.lng - 0.0013733 - 0.0001);

INSERT INTO geohash_latlng_nearby(
	`geohashed`,
	`geohashed_nearby`	
)SELECT
	t1.geohashed,
	t2.geohashed AS geohashed_nearby
FROM
	geohash_latlng AS t1,
	geohash_latlng AS t2
WHERE
   t2.lng = t1.lng AND t2.lat < (t1.lat + 0.00137329 + 0.0001) AND (t2.lat > t1.lat - 0.00137329 - 0.0001);
#每个单元格的长与宽都是0.0006866455078125*2=0.001373291015625，因为转换精度，lat使用0.00137329，lng使用0.0013733
#####################################################################################################
DROP TABLE
IF EXISTS `trainstart_trainend_count1_nearby_temp`;

CREATE TABLE trainstart_trainend_count1_nearby_temp SELECT
	`t1`.`geohashed_start_loc` AS `geohashed_start_loc`,
	`t1`.`geohashed_end_loc` AS `geohashed_end_loc`,
	`t1`.`start_lat` AS `start_lat`,
	`t1`.`start_lng` AS `start_lng`,
	`t1`.`end_lat` AS `end_lat`,
	`t1`.`end_lng` AS `end_lng`,
	`t1`.`startend_count1` AS `startend_count1`,
	`t2`.`startend_count1` AS `startend_count1_nearby`
FROM
	(
		(
			`trainstart_trainend_count1` `t1`
			JOIN `trainstart_trainend_count1` `t2` ON(
				(
					`t2`.`geohashed_end_loc` = `t1`.`geohashed_end_loc`
				)
			)
		)
		JOIN `geohash_latlng_nearby` ON(
			(
				(
					`geohash_latlng_nearby`.`geohashed` = `t1`.`geohashed_start_loc`
				)
				AND(
					`t2`.`geohashed_start_loc` = `geohash_latlng_nearby`.`geohashed_nearby`
				)
			)
		)
	);

ALTER TABLE `trainstart_trainend_count1_nearby_temp` ADD INDEX geohashed_start_loc(`geohashed_start_loc`);

ALTER TABLE `trainstart_trainend_count1_nearby_temp` ADD INDEX geohashed_end_loc(`geohashed_end_loc`);

########################################################################################
DROP TABLE
IF EXISTS `trainstart_trainend_count1_nearby`;

CREATE TABLE trainstart_trainend_count1_nearby SELECT
	`geohashed_start_loc`,
	`geohashed_end_loc`,
	`start_lat`,
	`start_lng`,
	`end_lat`,
	`end_lng`,
	sum(`startend_count1_nearby`)AS startend_count1
FROM
	trainstart_trainend_count1_nearby_temp
GROUP BY
	geohashed_start_loc,
	geohashed_end_loc
ORDER BY
	startend_count1_nearby DESC;

ALTER TABLE trainstart_trainend_count1_nearby ADD PRIMARY KEY(
	`geohashed_start_loc`,
	`geohashed_end_loc`
);

DROP TABLE trainstart_trainend_count1_nearby_temp;

#########################################################################################
######################################################
DROP TABLE
IF EXISTS `trainstart_trainend_count2_nearby`;

CREATE TABLE trainstart_trainend_count2_nearby SELECT
	geohashed_start_loc,
	start_lat,
	start_lng,
	sum(startend_count1)AS startend_count2
FROM
	trainstart_trainend_count1_nearby
GROUP BY
	geohashed_start_loc
ORDER BY
	startend_count2 DESC;

ALTER TABLE trainstart_trainend_count2_nearby ADD PRIMARY KEY(`geohashed_start_loc`);

ALTER TABLE `trainstart_trainend_count2_nearby` ADD INDEX start_lat(`start_lat`);

ALTER TABLE `trainstart_trainend_count2_nearby` ADD INDEX start_lng(`start_lng`);

####################生成训练集中所有从起点到某目标点的概率值###############
####################修正系数3X3的为0.552135846231146#######################
####################修正系数cross的为0.579384554753277#####################
DROP TABLE
IF EXISTS `trainstart_trainend_probability`;

CREATE TABLE trainstart_trainend_probability SELECT
	trainstart_trainend_count1_nearby.geohashed_start_loc,
	trainstart_trainend_count1_nearby.geohashed_end_loc,
	trainstart_trainend_count1_nearby.startend_count1 / trainstart_trainend_count2_nearby.startend_count2 * 0.614889663 AS probability
FROM
	trainstart_trainend_count1_nearby
INNER JOIN trainstart_trainend_count2_nearby ON trainstart_trainend_count1_nearby.geohashed_start_loc = trainstart_trainend_count2_nearby.geohashed_start_loc
ORDER BY
	probability DESC;

ALTER TABLE trainstart_trainend_probability ADD PRIMARY KEY(
	`geohashed_start_loc`,
	`geohashed_end_loc`
);

DROP TABLE trainstart_trainend_count1_nearby;

DROP TABLE trainstart_trainend_count2_nearby;

####################保存一个预测概率的备份，因为生成三列预测时候会依次删除3个最高预测###############
DROP TABLE
IF EXISTS `trainstart_trainend_probability_back_cross_pro`;

CREATE TABLE trainstart_trainend_probability_back_cross_pro SELECT
	*
FROM
	trainstart_trainend_probability;

ALTER TABLE trainstart_trainend_probability_back_cross_pro ADD PRIMARY KEY(
	`geohashed_start_loc`,
	`geohashed_end_loc`
);

######################读取每个起点到某目标点最高概率值##########
DROP TABLE
IF EXISTS `trainstart_trainend_probability_top1`;

CREATE TABLE trainstart_trainend_probability_top1 SELECT
	*
FROM
	trainstart_trainend_probability
GROUP BY
	geohashed_start_loc;

ALTER TABLE trainstart_trainend_probability_top1 ADD PRIMARY KEY(
	`geohashed_start_loc`,
	`geohashed_end_loc`
);

DELETE
FROM
	trainstart_trainend_probability
WHERE
	(
		geohashed_start_loc,
		geohashed_end_loc
	)IN(
		SELECT
			geohashed_start_loc,
			geohashed_end_loc
		FROM
			trainstart_trainend_probability_top1
	);

######################读取每个起点到某目标点次高概率值##########
DROP TABLE
IF EXISTS `trainstart_trainend_probability_top2`;

CREATE TABLE trainstart_trainend_probability_top2 SELECT
	*
FROM
	trainstart_trainend_probability
GROUP BY
	geohashed_start_loc;

ALTER TABLE trainstart_trainend_probability_top2 ADD PRIMARY KEY(
	`geohashed_start_loc`,
	`geohashed_end_loc`
);

DELETE
FROM
	trainstart_trainend_probability
WHERE
	(
		geohashed_start_loc,
		geohashed_end_loc
	)IN(
		SELECT
			geohashed_start_loc,
			geohashed_end_loc
		FROM
			trainstart_trainend_probability_top2
	);

######################读取每个起点到某目标点第三高概率值########
DROP TABLE
IF EXISTS `trainstart_trainend_probability_top3`;

CREATE TABLE trainstart_trainend_probability_top3 SELECT
	*
FROM
	trainstart_trainend_probability
GROUP BY
	geohashed_start_loc;

ALTER TABLE trainstart_trainend_probability_top3 ADD PRIMARY KEY(
	`geohashed_start_loc`,
	`geohashed_end_loc`
);

DELETE
FROM
	trainstart_trainend_probability
WHERE
	(
		geohashed_start_loc,
		geohashed_end_loc
	)IN(
		SELECT
			geohashed_start_loc,
			geohashed_end_loc
		FROM
			trainstart_trainend_probability_top3
	);

DROP TABLE trainstart_trainend_probability;

######################生成预测结果，带概率值##########################
DROP TABLE
IF EXISTS `final_probability_trainstart_trainend_nearby_cross_pro`;

CREATE TABLE final_probability_trainstart_trainend_nearby_cross_pro SELECT
	`test`.`orderid` AS `orderid`,
	`trainstart_trainend_probability_top1`.`geohashed_end_loc` AS `geohashed_end_loc1`,
	`trainstart_trainend_probability_top1`.`probability` AS `geohashed_end_probability1`,
	`trainstart_trainend_probability_top2`.`geohashed_end_loc` AS `geohashed_end_loc2`,
	`trainstart_trainend_probability_top2`.`probability` AS `geohashed_end_probability2`,
	`trainstart_trainend_probability_top3`.`geohashed_end_loc` AS `geohashed_end_loc3`,
	`trainstart_trainend_probability_top3`.`probability` AS `geohashed_end_probability3`
FROM
	(
		(
			(
				`test`
				LEFT JOIN `trainstart_trainend_probability_top1` ON(
					(
						`trainstart_trainend_probability_top1`.`geohashed_start_loc` = `test`.`geohashed_start_loc`
					)
				)
			)
			LEFT JOIN `trainstart_trainend_probability_top2` ON(
				(
					`trainstart_trainend_probability_top2`.`geohashed_start_loc` = `test`.`geohashed_start_loc`
				)
			)
		)
		LEFT JOIN `trainstart_trainend_probability_top3` ON(
			(
				`trainstart_trainend_probability_top3`.`geohashed_start_loc` = `test`.`geohashed_start_loc`
			)
		)
	);

######################生成结果，缺失值随机填充wx4sqpb########
DROP TABLE
IF EXISTS `final_trainstart_trainend_nearby_cross_pro`;

CREATE TABLE final_trainstart_trainend_nearby_cross_pro SELECT
	`test`.`orderid` AS `orderid`,
	`trainstart_trainend_probability_top1`.`geohashed_end_loc` AS `geohashed_end_loc1`,
	`trainstart_trainend_probability_top2`.`geohashed_end_loc` AS `geohashed_end_loc2`,
	`trainstart_trainend_probability_top3`.`geohashed_end_loc` AS `geohashed_end_loc3`
FROM
	(
		(
			(
				`test`
				LEFT JOIN `trainstart_trainend_probability_top1` ON(
					(
						`trainstart_trainend_probability_top1`.`geohashed_start_loc` = `test`.`geohashed_start_loc`
					)
				)
			)
			LEFT JOIN `trainstart_trainend_probability_top2` ON(
				(
					`trainstart_trainend_probability_top2`.`geohashed_start_loc` = `test`.`geohashed_start_loc`
				)
			)
		)
		LEFT JOIN `trainstart_trainend_probability_top3` ON(
			(
				`trainstart_trainend_probability_top3`.`geohashed_start_loc` = `test`.`geohashed_start_loc`
			)
		)
	);

UPDATE final_trainstart_trainend_nearby_cross_pro
SET geohashed_end_loc1 = 'wx4sqpb'
WHERE
	ISNULL(geohashed_end_loc1);

UPDATE final_trainstart_trainend_nearby_cross_pro
SET geohashed_end_loc2 = 'wx4sqpb'
WHERE
	ISNULL(geohashed_end_loc2);

UPDATE final_trainstart_trainend_nearby_cross_pro
SET geohashed_end_loc3 = 'wx4sqpb'
WHERE
	ISNULL(geohashed_end_loc3);

DROP TABLE trainstart_trainend_probability_top1;

DROP TABLE trainstart_trainend_probability_top2;

DROP TABLE trainstart_trainend_probability_top3;

DROP TABLE trainstart_trainend_count1;

DROP TABLE geohash_latlng_nearby;

#参考语句
#ALTER TABLE `table_name` ADD PRIMARY KEY ( `column` )
#ALTER TABLE `table_name` ADD UNIQUE (`column`)
#ALTER TABLE `table_name` ADD INDEX index_name ( `column` )




############################################################################
########################Calculating_probability_userid_startandend_3########
############################################################################
#######程序需要的表#############
#train_complete
#test_complete
#train_m_distance_count
################################
#######程序生成的表#############
#test_userid_startandend_probability_back_3
#final_probability_userid_startandend_3
#final_userid_startandend_3
################################
####################汇总所有出现过的开始及结束位置并存为表userid_startandend_complete###############################################
DROP TABLE
IF EXISTS `userid_startandend_complete`;

CREATE TABLE `userid_startandend_complete`(
	`orderid` INT(11)DEFAULT NULL,
	`userid` INT(11)DEFAULT NULL,
	`geohashed_loc` VARCHAR(7)NOT NULL,
	`lat` DOUBLE NOT NULL,
	`lng` DOUBLE NOT NULL,
	`starttime` datetime NOT NULL,
	`m_distance` DOUBLE,
	`rushhour` INT(11)NOT NULL,
	`start_or_end` INT(1)NOT NULL DEFAULT '0'
)ENGINE = MyISAM DEFAULT CHARSET = ascii;

INSERT INTO userid_startandend_complete(
	`orderid`,
	`userid`,
	`geohashed_loc`,
	`lat`,
	`lng`,
	`starttime`,
	`m_distance`,
	`rushhour`,
	`start_or_end`
)SELECT
	train_complete.orderid,
	train_complete.userid,
	train_complete.geohashed_start_loc,
	train_complete.start_lat,
	train_complete.start_lng,
	train_complete.starttime,
	train_complete.m_distance,
	train_complete.rushhour,
	0
FROM
	train_complete;

INSERT INTO userid_startandend_complete(
	`orderid`,
	`userid`,
	`geohashed_loc`,
	`lat`,
	`lng`,
	`starttime`,
	`m_distance`,
	`rushhour`,
	`start_or_end`
)SELECT
	train_complete.orderid,
	train_complete.userid,
	train_complete.geohashed_end_loc,
	train_complete.end_lat,
	train_complete.end_lng,
	train_complete.starttime,
	train_complete.m_distance,
	train_complete.rushhour,
	1
FROM
	train_complete;

INSERT INTO userid_startandend_complete(
	`orderid`,
	`userid`,
	`geohashed_loc`,
	`lat`,
	`lng`,
	`starttime`,
	`rushhour`,
	`start_or_end`
)SELECT
	test_complete.orderid,
	test_complete.userid,
	test_complete.geohashed_start_loc,
	test_complete.start_lat,
	test_complete.start_lng,
	test_complete.starttime,
	test_complete.rushhour,
	0
FROM
	test_complete;

ALTER TABLE userid_startandend_complete ADD PRIMARY KEY(`orderid`, `start_or_end`);

ALTER TABLE `userid_startandend_complete` ADD INDEX userid(`userid`);

ALTER TABLE `userid_startandend_complete` ADD INDEX geohashed_loc(`geohashed_loc`);

#################################统计每个用户所涉及的位置及出现频率#############################################
DROP TABLE
IF EXISTS `userid_startandend_complete_count1`;

CREATE TABLE userid_startandend_complete_count1 SELECT
	userid,
	geohashed_loc,
	lat,
	lng,
	avg(m_distance)AS m_distance_avg,
	count(*)AS user_geo_count
FROM
	userid_startandend_complete
GROUP BY
	userid,
	geohashed_loc;

ALTER TABLE userid_startandend_complete_count1 ADD PRIMARY KEY(`userid`, `geohashed_loc`);

###################################统计只出现过一个位置的用户，以备删除########################################################
DROP TABLE
IF EXISTS `userid_startandend_complete_count1_temp`;

CREATE TABLE userid_startandend_complete_count1_temp SELECT
	userid,
	count(*)AS usergeo_count
FROM
	userid_startandend_complete_count1
GROUP BY
	userid;

DELETE
FROM
	userid_startandend_complete_count1_temp
WHERE
	usergeo_count <> 1;

ALTER TABLE userid_startandend_complete_count1_temp ADD PRIMARY KEY(`userid`);

##################################删除userid_startandend_complete_count1表中只出现过一个位置的用户#############################
DELETE
FROM
	userid_startandend_complete_count1
WHERE
	(`userid`)IN(
		SELECT
			`userid`
		FROM
			userid_startandend_complete_count1_temp
	);

DROP TABLE userid_startandend_complete_count1_temp;

####################生成结果集中每条记录中，这个用户曾经去过的所有点（包含去过的次数），作为备选目标点，并计算其与出发点距离###############
##################################test_complete表与userid_startandend_complete_count1合#########################################################
DROP TABLE
IF EXISTS `test_userid_startandend_count1`;

CREATE TABLE test_userid_startandend_count1 SELECT
	t1.orderid,
	t1.userid,
	t1.geohashed_start_loc,
	t1.start_lat,
	t1.start_lng,
	u1.geohashed_loc AS geohashed_end_loc,
	floor(
		((u1.m_distance_avg) + 50)/ 100
	)AS m_distance_avg100,
	floor(
		(
			(
				abs(t1.start_lat - u1.lat)* 111700 + abs(t1.start_lng - u1.lng)* 85567
			)+ 50
		)/ 100
	)AS m_distance100,
	u1.user_geo_count
FROM
	test_complete AS t1
INNER JOIN userid_startandend_complete_count1 AS u1 ON u1.userid = t1.userid
AND u1.geohashed_loc <> t1.geohashed_start_loc;

ALTER TABLE `test_userid_startandend_count1` ADD PRIMARY KEY(
	`orderid`,
	`geohashed_end_loc`
);

ALTER TABLE `test_userid_startandend_count1` ADD INDEX m_distance100(`m_distance100`);

###################test_userid_startandend_count1与train_m_distance_count合，将距离转换为概率，并初步计算出该概率############
DROP TABLE
IF EXISTS `test_userid_startandend_count2`;

CREATE TABLE test_userid_startandend_count2 SELECT
	test_userid_startandend_count1.orderid,
	test_userid_startandend_count1.userid,
	test_userid_startandend_count1.geohashed_start_loc,
	test_userid_startandend_count1.start_lat,
	test_userid_startandend_count1.start_lng,
	test_userid_startandend_count1.geohashed_end_loc,
	test_userid_startandend_count1.m_distance_avg100,
	test_userid_startandend_count1.m_distance100,
	test_userid_startandend_count1.user_geo_count,
	train_m_distance_count.m_distance_percent,
	test_userid_startandend_count1.user_geo_count * train_m_distance_count.m_distance_percent AS probability_base
FROM
	test_userid_startandend_count1
INNER JOIN train_m_distance_count ON train_m_distance_count.m_distance100 = test_userid_startandend_count1.m_distance100;

ALTER TABLE `test_userid_startandend_count2` ADD PRIMARY KEY(
	`orderid`,
	`geohashed_end_loc`
);

ALTER TABLE `test_userid_startandend_count2` ADD INDEX m_distance_percent(`m_distance_percent`);

###########################################计算该订单下的所有可能目标的概率和#########################################################
DROP TABLE
IF EXISTS `test_userid_startandend_count3`;

CREATE TABLE test_userid_startandend_count3 SELECT
	test_userid_startandend_count2.orderid,
	sum(
		test_userid_startandend_count2.probability_base
	)AS probability_base_sum,
	max(
		test_userid_startandend_count2.m_distance_percent
	)AS m_distance_percent_max

FROM
	test_userid_startandend_count2
GROUP BY
	orderid;

ALTER TABLE `test_userid_startandend_count3` ADD PRIMARY KEY(`orderid`);

######################生成预测概率表##################################################################
DROP TABLE
IF EXISTS `test_userid_startandend_probability`;

CREATE TABLE test_userid_startandend_probability SELECT
	test_userid_startandend_count2.orderid,
	test_userid_startandend_count2.userid,
	test_userid_startandend_count2.geohashed_start_loc,
	test_userid_startandend_count2.start_lat,
	test_userid_startandend_count2.start_lng,
	test_userid_startandend_count2.geohashed_end_loc,
	test_userid_startandend_count2.m_distance_avg100,
	test_userid_startandend_count2.m_distance100,
	test_userid_startandend_count2.user_geo_count,
	test_userid_startandend_count2.m_distance_percent,
	test_userid_startandend_count2.probability_base,
	test_userid_startandend_count3.probability_base_sum,
	test_userid_startandend_count2.probability_base / test_userid_startandend_count3.probability_base_sum * m_distance_percent_max *0.688632205413756 AS probability
FROM
	test_userid_startandend_count2
INNER JOIN test_userid_startandend_count3 ON test_userid_startandend_count3.orderid = test_userid_startandend_count2.orderid
ORDER BY
	probability DESC;

ALTER TABLE `test_userid_startandend_probability` ADD PRIMARY KEY(
	`orderid`,
	`geohashed_end_loc`
);

DROP TABLE userid_startandend_complete;

DROP TABLE userid_startandend_complete_count1;

DROP TABLE test_userid_startandend_count1;

DROP TABLE test_userid_startandend_count2;

DROP TABLE test_userid_startandend_count3;

######################生成预测概率的备份表##################################################################
DROP TABLE
IF EXISTS `test_userid_startandend_probability_back_3`;

CREATE TABLE test_userid_startandend_probability_back_3 SELECT
	*
FROM
	test_userid_startandend_probability;

ALTER TABLE `test_userid_startandend_probability_back_3` ADD PRIMARY KEY(
	`orderid`,
	`geohashed_end_loc`
);

######################读取每个起点到某目标点最高概率值##########
DROP TABLE
IF EXISTS `test_userid_startandend_probability_top1`;

CREATE TABLE test_userid_startandend_probability_top1 SELECT
	*
FROM
	test_userid_startandend_probability
GROUP BY
	orderid;

ALTER TABLE test_userid_startandend_probability_top1 ADD PRIMARY KEY(
	`orderid`,
	`geohashed_end_loc`
);

DELETE
FROM
	test_userid_startandend_probability
WHERE
	(
		`orderid`,
		`geohashed_end_loc`
	)IN(
		SELECT
			`orderid`,
			`geohashed_end_loc`
		FROM
			test_userid_startandend_probability_top1
	);

######################读取每个起点到某目标点次高概率值##########
DROP TABLE
IF EXISTS `test_userid_startandend_probability_top2`;

CREATE TABLE test_userid_startandend_probability_top2 SELECT
	*
FROM
	test_userid_startandend_probability
GROUP BY
	orderid;

ALTER TABLE test_userid_startandend_probability_top2 ADD PRIMARY KEY(
	`orderid`,
	`geohashed_end_loc`
);

DELETE
FROM
	test_userid_startandend_probability
WHERE
	(
		`orderid`,
		`geohashed_end_loc`
	)IN(
		SELECT
			`orderid`,
			`geohashed_end_loc`
		FROM
			test_userid_startandend_probability_top2
	);

######################读取每个起点到某目标点第三高概率值########
DROP TABLE
IF EXISTS `test_userid_startandend_probability_top3`;

CREATE TABLE test_userid_startandend_probability_top3 SELECT
	*
FROM
	test_userid_startandend_probability
GROUP BY
	orderid;

ALTER TABLE test_userid_startandend_probability_top3 ADD PRIMARY KEY(
	`orderid`,
	`geohashed_end_loc`
);

DELETE
FROM
	test_userid_startandend_probability
WHERE
	(
		`orderid`,
		`geohashed_end_loc`
	)IN(
		SELECT
			`orderid`,
			`geohashed_end_loc`
		FROM
			test_userid_startandend_probability_top3
	);

DROP TABLE test_userid_startandend_probability;

######################生成预测结果，带概率值##########################
DROP TABLE
IF EXISTS `final_probability_userid_startandend_3`;

CREATE TABLE final_probability_userid_startandend_3 SELECT
	`test`.`orderid` AS `orderid`,
	`test_userid_startandend_probability_top1`.`geohashed_end_loc` AS `geohashed_end_loc1`,
	`test_userid_startandend_probability_top1`.`probability` AS `geohashed_end_probability1`,
	`test_userid_startandend_probability_top2`.`geohashed_end_loc` AS `geohashed_end_loc2`,
	`test_userid_startandend_probability_top2`.`probability` AS `geohashed_end_probability2`,
	`test_userid_startandend_probability_top3`.`geohashed_end_loc` AS `geohashed_end_loc3`,
	`test_userid_startandend_probability_top3`.`probability` AS `geohashed_end_probability3`
FROM
	(
		(
			(
				`test`
				LEFT JOIN `test_userid_startandend_probability_top1` ON(
					(
						`test_userid_startandend_probability_top1`.`orderid` = `test`.`orderid`
					)
				)
			)
			LEFT JOIN `test_userid_startandend_probability_top2` ON(
				(
					`test_userid_startandend_probability_top2`.`orderid` = `test`.`orderid`
				)
			)
		)
		LEFT JOIN `test_userid_startandend_probability_top3` ON(
			(
				`test_userid_startandend_probability_top3`.`orderid` = `test`.`orderid`
			)
		)
	);

######################生成结果，缺失值随机填充wx4sqpb########
DROP TABLE
IF EXISTS `final_userid_startandend_3`;

CREATE TABLE final_userid_startandend_3 SELECT
	`test`.`orderid` AS `orderid`,
	`test_userid_startandend_probability_top1`.`geohashed_end_loc` AS `geohashed_end_loc1`,
	`test_userid_startandend_probability_top2`.`geohashed_end_loc` AS `geohashed_end_loc2`,
	`test_userid_startandend_probability_top3`.`geohashed_end_loc` AS `geohashed_end_loc3`
FROM
	(
		(
			(
				`test`
				LEFT JOIN `test_userid_startandend_probability_top1` ON(
					(
						`test_userid_startandend_probability_top1`.`orderid` = `test`.`orderid`
					)
				)
			)
			LEFT JOIN `test_userid_startandend_probability_top2` ON(
				(
					`test_userid_startandend_probability_top2`.`orderid` = `test`.`orderid`
				)
			)
		)
		LEFT JOIN `test_userid_startandend_probability_top3` ON(
			(
				`test_userid_startandend_probability_top3`.`orderid` = `test`.`orderid`
			)
		)
	);

UPDATE final_userid_startandend_3
SET geohashed_end_loc1 = 'wx4sqpb'
WHERE
	ISNULL(geohashed_end_loc1);

UPDATE final_userid_startandend_3
SET geohashed_end_loc2 = 'wx4sqpb'
WHERE
	ISNULL(geohashed_end_loc2);

UPDATE final_userid_startandend_3
SET geohashed_end_loc3 = 'wx4sqpb'
WHERE
	ISNULL(geohashed_end_loc3);

DROP TABLE test_userid_startandend_probability_top1;

DROP TABLE test_userid_startandend_probability_top2;

DROP TABLE test_userid_startandend_probability_top3;


############################################################################
########################Calculating_probability_userid_starttoend_2#########
############################################################################

#######程序需要的表#############
#train_complete
#test_complete
#geohash_latlng
################################
#######程序生成的表#############
#test_userid_starttoend_probability_back_2
#final_probability_userid_starttoend_2
#final_userid_starttoend_2
################################

#########################################################################################################
###############继续采用cross_pro核，中间的位置权重乘2，计算出所有位置的直接临近关系###########################
###############并将结果保存在geohash_latlng_nearby表中，其他程序也会生成和使用同名文件，可以复用##############
#########################################################################################################
DROP TABLE
IF EXISTS `geohash_latlng_nearby`;

CREATE TABLE geohash_latlng_nearby SELECT
	t1.geohashed,
	t2.geohashed AS geohashed_nearby
FROM
	geohash_latlng AS t1,
	geohash_latlng AS t2
WHERE
   t2.lat = t1.lat AND t2.lng < (t1.lng + 0.0013733 + 0.0001) AND (t2.lng > t1.lng - 0.0013733 - 0.0001);

INSERT INTO geohash_latlng_nearby(
	`geohashed`,
	`geohashed_nearby`	
)SELECT
	t1.geohashed,
	t2.geohashed AS geohashed_nearby
FROM
	geohash_latlng AS t1,
	geohash_latlng AS t2
WHERE
   t2.lng = t1.lng AND t2.lat < (t1.lat + 0.00137329 + 0.0001) AND (t2.lat > t1.lat - 0.00137329 - 0.0001);


ALTER TABLE geohash_latlng_nearby ADD INDEX geohashed(`geohashed`);
ALTER TABLE geohash_latlng_nearby ADD INDEX geohashed_nearby(`geohashed_nearby`);


############生成train表中所有用户的userid列表及对应的记录数量###################################
DROP TABLE
IF EXISTS `train_userid_count`;

CREATE TABLE train_userid_count SELECT
	userid,
	count(*)AS train_userid_count
FROM
	train_complete
GROUP BY
	userid;

ALTER TABLE train_userid_count ADD PRIMARY KEY(`userid`);

############生成test表中所有用户的userid列表及对应的记录数量###################################
DROP TABLE
IF EXISTS `test_userid_count`;

CREATE TABLE test_userid_count SELECT
	userid,
	count(*)AS test_userid_count
FROM
	test_complete
GROUP BY
	userid;

ALTER TABLE test_userid_count ADD PRIMARY KEY(`userid`);

##############生成train表哪些userid在对应的有test表中出现#################################
DROP TABLE
IF EXISTS `train_test_userid_count`;

CREATE TABLE train_test_userid_count SELECT
train_userid_count.userid,
train_userid_count.train_userid_count,
test_userid_count.test_userid_count
FROM
train_userid_count
LEFT JOIN test_userid_count ON test_userid_count.userid = train_userid_count.userid;

##############生成test表哪些userid在对应的有train表中出现#################################
DROP TABLE
IF EXISTS `test_train_userid_count`;

CREATE TABLE test_train_userid_count SELECT
test_userid_count.userid,
test_userid_count.test_userid_count,
train_userid_count.train_userid_count
FROM
test_userid_count
LEFT JOIN train_userid_count ON train_userid_count.userid = test_userid_count.userid;

##############筛选出train表中那些useid在test中出现的记录，以减少数据量，本表作为基本表使用##########################
DROP TABLE
IF EXISTS `train_userid_in_test`;

CREATE TABLE train_userid_in_test SELECT
train_complete.*
FROM
train_complete
inner JOIN test_userid_count ON test_userid_count.userid = train_complete.userid;

ALTER TABLE train_userid_in_test ADD PRIMARY KEY(`orderid`);

ALTER TABLE train_userid_in_test ADD INDEX userid(`userid`);

ALTER TABLE train_userid_in_test ADD INDEX geohashed_start_loc(`geohashed_start_loc`);

ALTER TABLE train_userid_in_test ADD INDEX geohashed_end_loc(`geohashed_end_loc`);


##############筛选出test表中那些useid在train中出现的记录，以减少数据量，本表作为基本表使用##########################
DROP TABLE
IF EXISTS `test_userid_in_train`;

CREATE TABLE test_userid_in_train SELECT
test_complete.*
FROM
test_complete
inner JOIN train_userid_count ON train_userid_count.userid = test_complete.userid;

ALTER TABLE test_userid_in_train ADD PRIMARY KEY(`orderid`);

ALTER TABLE test_userid_in_train ADD INDEX userid(`userid`);

ALTER TABLE test_userid_in_train ADD INDEX geohashed_start_loc(`geohashed_start_loc`);

##############对train_userid_in_test按照useid和开始位置分组汇总###################################################
DROP TABLE
IF EXISTS `train_userid_start_count`;

create table train_userid_start_count
select userid,geohashed_start_loc,count(*) as start_count
from train_userid_in_test
GROUP BY userid,geohashed_start_loc;

ALTER TABLE train_userid_start_count ADD PRIMARY KEY(`userid`,`geohashed_start_loc`);

##############对test_userid_in_test按照useid和开始位置分组汇总###################################################
DROP TABLE
IF EXISTS `test_userid_start_count`;

create table test_userid_start_count
select userid,geohashed_start_loc,count(*) as start_count
from test_userid_in_train
GROUP BY userid,geohashed_start_loc;

ALTER TABLE test_userid_start_count ADD PRIMARY KEY(`userid`,`geohashed_start_loc`);

##############对train_userid_in_test按照useid和开始位置，结束位置分组汇总##########################################
DROP TABLE
IF EXISTS `train_userid_start_to_end_count`;

create table train_userid_start_to_end_count
select userid,geohashed_start_loc,geohashed_end_loc,count(*) as start_to_end_count
from train_userid_in_test
GROUP BY userid,geohashed_start_loc,geohashed_end_loc;

ALTER TABLE train_userid_start_to_end_count ADD PRIMARY KEY(`userid`,`geohashed_start_loc`,`geohashed_end_loc`);

##############无法对test_userid_in_test按照useid和开始位置，结束位置分组汇总######################################

###############################################################################################################################

#####################生成train表的所有用户每个起点及其临近点#####################################################
DROP TABLE
IF EXISTS `train_userid_start_nearby_count`;

CREATE TABLE train_userid_start_nearby_count SELECT
	`train_userid_start_count`.`userid` AS `userid`,
	`train_userid_start_count`.`geohashed_start_loc` AS `geohashed_start_loc`,
	`train_userid_start_count`.`start_count` AS `start_count`,
	`geohash_latlng_nearby`.`geohashed_nearby` AS `geohashed_nearby`
FROM
	(
		`train_userid_start_count`
		JOIN `geohash_latlng_nearby` ON(
			(
				`geohash_latlng_nearby`.`geohashed` = `train_userid_start_count`.`geohashed_start_loc`
			)
		)
	);

ALTER TABLE train_userid_start_nearby_count ADD INDEX userid(`userid`);
ALTER TABLE train_userid_start_nearby_count ADD INDEX geohashed_start_loc(`geohashed_start_loc`);
ALTER TABLE train_userid_start_nearby_count ADD INDEX geohashed_nearby(`geohashed_nearby`);

#####################生成test表的所有用户每个起点及其临近点#####################################################
DROP TABLE
IF EXISTS `test_userid_start_nearby_count`;

CREATE TABLE test_userid_start_nearby_count SELECT
	`test_userid_start_count`.`userid` AS `userid`,
	`test_userid_start_count`.`geohashed_start_loc` AS `geohashed_start_loc`,
	`test_userid_start_count`.`start_count` AS `start_count`,
	`geohash_latlng_nearby`.`geohashed_nearby` AS `geohashed_nearby`
FROM
	(
		`test_userid_start_count`
		JOIN `geohash_latlng_nearby` ON(
			(
				`geohash_latlng_nearby`.`geohashed` = `test_userid_start_count`.`geohashed_start_loc`
			)
		)
	);

ALTER TABLE test_userid_start_nearby_count ADD INDEX userid(`userid`);
ALTER TABLE test_userid_start_nearby_count ADD INDEX geohashed_start_loc(`geohashed_start_loc`);
ALTER TABLE test_userid_start_nearby_count ADD INDEX geohashed_nearby(`geohashed_nearby`);

######################生成train表的所有用户每个起点及其临近点，结束点#########################################################
DROP TABLE
IF EXISTS `train_userid_start_nearby_to_end_count`;

CREATE TABLE train_userid_start_nearby_to_end_count SELECT
	train_userid_start_nearby_count.userid,
	train_userid_start_nearby_count.geohashed_start_loc,
	train_userid_start_to_end_count.geohashed_end_loc,
	train_userid_start_to_end_count.start_to_end_count
FROM
	train_userid_start_nearby_count
INNER JOIN train_userid_start_to_end_count ON train_userid_start_to_end_count.userid = train_userid_start_nearby_count.userid
AND train_userid_start_to_end_count.geohashed_start_loc = train_userid_start_nearby_count.geohashed_nearby;

ALTER TABLE train_userid_start_nearby_to_end_count ADD INDEX userid(`userid`);
ALTER TABLE train_userid_start_nearby_to_end_count ADD INDEX geohashed_start_loc(`geohashed_start_loc`);
ALTER TABLE train_userid_start_nearby_to_end_count ADD INDEX geohashed_end_loc(`geohashed_end_loc`);

######################生成test的起点及其临近的每个起点，连接train表的所有用户每个起点及其临近点，结束点############################

DROP TABLE
IF EXISTS `test_train_userid_start_nearby_to_end_count`;

CREATE TABLE test_train_userid_start_nearby_to_end_count SELECT
	test_userid_start_nearby_count.userid,
	test_userid_start_nearby_count.geohashed_start_loc,
	train_userid_start_nearby_to_end_count.geohashed_end_loc,
	train_userid_start_nearby_to_end_count.start_to_end_count
FROM
	test_userid_start_nearby_count
INNER JOIN train_userid_start_nearby_to_end_count ON train_userid_start_nearby_to_end_count.userid = test_userid_start_nearby_count.userid
AND train_userid_start_nearby_to_end_count.geohashed_start_loc = test_userid_start_nearby_count.geohashed_nearby;

ALTER TABLE test_train_userid_start_nearby_to_end_count ADD INDEX userid(`userid`);
ALTER TABLE test_train_userid_start_nearby_to_end_count ADD INDEX geohashed_start_loc(`geohashed_start_loc`);
ALTER TABLE test_train_userid_start_nearby_to_end_count ADD INDEX geohashed_end_loc(`geohashed_end_loc`);


####################生成test_train表中包含邻居数据的，某用户以某地为 出发地 及 目的地 的汇总数据#################################
####################本段的FROM为train_userid_start_nearby_to_end_count表示仅对train数据进行cross_pro卷########################
####################本段的FROM为test_train_userid_start_nearby_to_end_count表示对train及test数据进行cross_pro卷###############

DROP TABLE
IF EXISTS `t1`;

CREATE TABLE t1 SELECT
	userid,
	geohashed_start_loc,
	geohashed_end_loc,
	sum(start_to_end_count)AS start_to_end_count
FROM
	test_train_userid_start_nearby_to_end_count
GROUP BY
	userid,
	geohashed_start_loc,
	geohashed_end_loc;

ALTER TABLE t1 ADD PRIMARY KEY(`userid`,`geohashed_start_loc`,`geohashed_end_loc`);

####################生成test_train表中包含邻居数据的，某用户以某地为 出发地 的汇总数据#################################
DROP TABLE
IF EXISTS `t2`;

CREATE TABLE t2 SELECT
	userid,
	geohashed_start_loc,
	sum(start_to_end_count)AS start_count
FROM
	test_train_userid_start_nearby_to_end_count
GROUP BY
	userid,
	geohashed_start_loc;

ALTER TABLE t2 ADD PRIMARY KEY(`userid`,`geohashed_start_loc`);

#####################################################################################

####################生成训练集中选中用户从起点到某目标点的概率值###############
####################修正系数3X3的为#######################
####################修正系数cross的为#####################
DROP TABLE
IF EXISTS `trainstart_trainend_probability`;

CREATE TABLE trainstart_trainend_probability SELECT
	t1.userid,
	t1.geohashed_start_loc,
	t1.geohashed_end_loc,
	t1.start_to_end_count / t2.start_count * 0.591833043371423 AS probability
FROM
	t1
INNER JOIN t2 ON t1.userid = t2.userid and t1.geohashed_start_loc = t2.geohashed_start_loc
ORDER BY
	probability DESC;

ALTER TABLE trainstart_trainend_probability ADD PRIMARY KEY(
	`userid`,
	`geohashed_start_loc`,
	`geohashed_end_loc`
);

###################先计算一下简单的test出发位置完全吻合的预测，从其他程序直接copy的代码####################################
###################后面再研究一下test出发位置临近的预测####################################

DROP TABLE
IF EXISTS `test_userid_starttoend_probability`;

CREATE TABLE test_userid_starttoend_probability SELECT
test.orderid,
trainstart_trainend_probability.geohashed_end_loc,
trainstart_trainend_probability.probability

FROM
	test
INNER JOIN trainstart_trainend_probability ON test.userid = trainstart_trainend_probability.userid and test.geohashed_start_loc = trainstart_trainend_probability.geohashed_start_loc
ORDER BY
	probability DESC;

ALTER TABLE `test_userid_starttoend_probability` ADD PRIMARY KEY(
	`orderid`,
	`geohashed_end_loc`
);

######################生成预测概率的备份表##################################################################
DROP TABLE
IF EXISTS `test_userid_starttoend_probability_back_2`;

CREATE TABLE test_userid_starttoend_probability_back_2 SELECT
	*
FROM
	test_userid_starttoend_probability;

ALTER TABLE `test_userid_starttoend_probability_back_2` ADD PRIMARY KEY(
	`orderid`,
	`geohashed_end_loc`
);

######################读取每个起点到某目标点最高概率值##########
DROP TABLE
IF EXISTS `test_userid_starttoend_probability_top1`;

CREATE TABLE test_userid_starttoend_probability_top1 SELECT
	*
FROM
	test_userid_starttoend_probability
GROUP BY
	orderid;

ALTER TABLE test_userid_starttoend_probability_top1 ADD PRIMARY KEY(
	`orderid`,
	`geohashed_end_loc`
);

DELETE
FROM
	test_userid_starttoend_probability
WHERE
	(
		`orderid`,
		`geohashed_end_loc`
	)IN(
		SELECT
			`orderid`,
			`geohashed_end_loc`
		FROM
			test_userid_starttoend_probability_top1
	);

######################读取每个起点到某目标点次高概率值##########
DROP TABLE
IF EXISTS `test_userid_starttoend_probability_top2`;

CREATE TABLE test_userid_starttoend_probability_top2 SELECT
	*
FROM
	test_userid_starttoend_probability
GROUP BY
	orderid;

ALTER TABLE test_userid_starttoend_probability_top2 ADD PRIMARY KEY(
	`orderid`,
	`geohashed_end_loc`
);

DELETE
FROM
	test_userid_starttoend_probability
WHERE
	(
		`orderid`,
		`geohashed_end_loc`
	)IN(
		SELECT
			`orderid`,
			`geohashed_end_loc`
		FROM
			test_userid_starttoend_probability_top2
	);

######################读取每个起点到某目标点第三高概率值########
DROP TABLE
IF EXISTS `test_userid_starttoend_probability_top3`;

CREATE TABLE test_userid_starttoend_probability_top3 SELECT
	*
FROM
	test_userid_starttoend_probability
GROUP BY
	orderid;

ALTER TABLE test_userid_starttoend_probability_top3 ADD PRIMARY KEY(
	`orderid`,
	`geohashed_end_loc`
);

DELETE
FROM
	test_userid_starttoend_probability
WHERE
	(
		`orderid`,
		`geohashed_end_loc`
	)IN(
		SELECT
			`orderid`,
			`geohashed_end_loc`
		FROM
			test_userid_starttoend_probability_top3
	);

DROP TABLE test_userid_starttoend_probability;

######################生成预测结果，带概率值##########################
DROP TABLE
IF EXISTS `final_probability_userid_starttoend_2`;

CREATE TABLE final_probability_userid_starttoend_2 SELECT
	`test`.`orderid` AS `orderid`,
	`test_userid_starttoend_probability_top1`.`geohashed_end_loc` AS `geohashed_end_loc1`,
	`test_userid_starttoend_probability_top1`.`probability` AS `geohashed_end_probability1`,
	`test_userid_starttoend_probability_top2`.`geohashed_end_loc` AS `geohashed_end_loc2`,
	`test_userid_starttoend_probability_top2`.`probability` AS `geohashed_end_probability2`,
	`test_userid_starttoend_probability_top3`.`geohashed_end_loc` AS `geohashed_end_loc3`,
	`test_userid_starttoend_probability_top3`.`probability` AS `geohashed_end_probability3`
FROM
	(
		(
			(
				`test`
				LEFT JOIN `test_userid_starttoend_probability_top1` ON(
					(
						`test_userid_starttoend_probability_top1`.`orderid` = `test`.`orderid`
					)
				)
			)
			LEFT JOIN `test_userid_starttoend_probability_top2` ON(
				(
					`test_userid_starttoend_probability_top2`.`orderid` = `test`.`orderid`
				)
			)
		)
		LEFT JOIN `test_userid_starttoend_probability_top3` ON(
			(
				`test_userid_starttoend_probability_top3`.`orderid` = `test`.`orderid`
			)
		)
	);

######################生成结果，缺失值随机填充wx4sqpb########
DROP TABLE
IF EXISTS `final_userid_starttoend_2`;

CREATE TABLE final_userid_starttoend_2 SELECT
	`test`.`orderid` AS `orderid`,
	`test_userid_starttoend_probability_top1`.`geohashed_end_loc` AS `geohashed_end_loc1`,
	`test_userid_starttoend_probability_top2`.`geohashed_end_loc` AS `geohashed_end_loc2`,
	`test_userid_starttoend_probability_top3`.`geohashed_end_loc` AS `geohashed_end_loc3`
FROM
	(
		(
			(
				`test`
				LEFT JOIN `test_userid_starttoend_probability_top1` ON(
					(
						`test_userid_starttoend_probability_top1`.`orderid` = `test`.`orderid`
					)
				)
			)
			LEFT JOIN `test_userid_starttoend_probability_top2` ON(
				(
					`test_userid_starttoend_probability_top2`.`orderid` = `test`.`orderid`
				)
			)
		)
		LEFT JOIN `test_userid_starttoend_probability_top3` ON(
			(
				`test_userid_starttoend_probability_top3`.`orderid` = `test`.`orderid`
			)
		)
	);

UPDATE final_userid_starttoend_2
SET geohashed_end_loc1 = 'wx4sqpb'
WHERE
	ISNULL(geohashed_end_loc1);

UPDATE final_userid_starttoend_2
SET geohashed_end_loc2 = 'wx4sqpb'
WHERE
	ISNULL(geohashed_end_loc2);

UPDATE final_userid_starttoend_2
SET geohashed_end_loc3 = 'wx4sqpb'
WHERE
	ISNULL(geohashed_end_loc3);

DROP TABLE test_userid_starttoend_probability_top1;

DROP TABLE test_userid_starttoend_probability_top2;

DROP TABLE test_userid_starttoend_probability_top3;

#################################################



##################参考数据######################
########train数据量，及userid出现在test表中的次数，数量#################
###0	3214096
###1	2716338
###2	2293163
###3	1870643
###4	1496836
###5	1182314
###6	931711
###7	734987
###8	584207
###9	465622
###10	371254

########test数据量，及userid出现在train表中的次数，数量##################
###0	2002996
###1	979717
###2	956007
###3	927195
###4	893706
###5	855325
###6	812201
###7	765092
###8	715088
###9	662110
###10	610273


DROP TABLE
IF EXISTS `geohash_latlng_nearby`;

DROP TABLE
IF EXISTS `train_userid_count`;

DROP TABLE
IF EXISTS `test_userid_count`;

DROP TABLE
IF EXISTS `train_test_userid_count`;

DROP TABLE
IF EXISTS `test_train_userid_count`;

DROP TABLE
IF EXISTS `train_userid_in_test`;

DROP TABLE
IF EXISTS `test_userid_in_train`;

DROP TABLE
IF EXISTS `train_userid_start_count`;

DROP TABLE
IF EXISTS `test_userid_start_count`;

DROP TABLE
IF EXISTS `train_userid_start_to_end_count`;

DROP TABLE
IF EXISTS `train_userid_start_nearby_count`;

DROP TABLE
IF EXISTS `test_userid_start_nearby_count`;

DROP TABLE
IF EXISTS `train_userid_start_nearby_to_end_count`;

DROP TABLE
IF EXISTS `t1`;

DROP TABLE
IF EXISTS `t2`;

DROP TABLE
IF EXISTS `trainstart_trainend_probability`;

DROP TABLE
IF EXISTS `test_train_userid_start_nearby_to_end_count`;



############################################################################
########################Merge_probability_max_4#############################
############################################################################

########################################################################################################
DROP TABLE
IF EXISTS `probability_mix_4_temp`;

CREATE TABLE `probability_mix_4_temp` (
  `finalid` int(11) NOT NULL AUTO_INCREMENT,
  `orderid` int(11) NOT NULL DEFAULT '0',
  `geohashed_end_loc` varchar(7) DEFAULT '',
  `geohashed_end_probability` decimal(62,19) DEFAULT NULL,
  `geohashed_from` varchar(100) DEFAULT '',
  PRIMARY KEY (`finalid`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=ascii;

#############导入基于userid，来自train表中的所有出发地和到达地的预测结果####################################
#############第一列预测概率	0.428019948228863
#############第一列覆盖概率	0.301177836
#############导入final_probability_userid_starttoend_2表################################################
INSERT INTO probability_mix_4_temp(
	`orderid`,
	`geohashed_end_loc`,
	`geohashed_end_probability`,
	`geohashed_from`
)SELECT
	`orderid`,
	`geohashed_end_loc1`,
	`geohashed_end_probability1`,
	'final_probability_userid_starttoend_2'
FROM
	`final_probability_userid_starttoend_2`;

INSERT INTO probability_mix_4_temp(
	`orderid`,
	`geohashed_end_loc`,
	`geohashed_end_probability`,
	`geohashed_from`
)SELECT
	`orderid`,
	`geohashed_end_loc2`,
	`geohashed_end_probability2`,
	'final_probability_userid_starttoend_2'
FROM
	`final_probability_userid_starttoend_2`;

INSERT INTO probability_mix_4_temp(
	`orderid`,
	`geohashed_end_loc`,
	`geohashed_end_probability`,
	`geohashed_from`
)SELECT
	`orderid`,
	`geohashed_end_loc3`,
	`geohashed_end_probability3`,
	'final_probability_userid_starttoend_2'
FROM
	`final_probability_userid_starttoend_2`;

#############导入基于userid，来自train表中的所有出发地和到达地正向关系的预测结果#############################
#############第一列预测概率	0.245122722192525
#############第一列覆盖概率	0.704803205
#############导入final_probability_userid_startandend_3表################################################
INSERT INTO probability_mix_4_temp(
	`orderid`,
	`geohashed_end_loc`,
	`geohashed_end_probability`,
	`geohashed_from`
)SELECT
	`orderid`,
	`geohashed_end_loc1`,
	`geohashed_end_probability1`,
	'final_probability_userid_startandend_3'
FROM
	`final_probability_userid_startandend_3`;

INSERT INTO probability_mix_4_temp(
	`orderid`,
	`geohashed_end_loc`,
	`geohashed_end_probability`,
	`geohashed_from`
)SELECT
	`orderid`,
	`geohashed_end_loc2`,
	`geohashed_end_probability2`,
	'final_probability_userid_startandend_3'
FROM
	`final_probability_userid_startandend_3`;

INSERT INTO probability_mix_4_temp(
	`orderid`,
	`geohashed_end_loc`,
	`geohashed_end_probability`,
	`geohashed_from`
)SELECT
	`orderid`,
	`geohashed_end_loc3`,
	`geohashed_end_probability3`,
	'final_probability_userid_startandend_3'
FROM
	`final_probability_userid_startandend_3`;
#############导入基于test表中的所有车辆信息的预测结果#######################################################
#############第一列预测概率	0.142872140336175
#############第一列覆盖概率	0.765636576
#############导入final_probability_test_bike表############################################################
INSERT INTO probability_mix_4_temp(
	`orderid`,
	`geohashed_end_loc`,
	`geohashed_end_probability`,
	`geohashed_from`
)SELECT
	`orderid`,
	`geohashed_end_loc1`,
	`geohashed_end_probability1`,
	'final_probability_test_bike_correct'
FROM
	`final_probability_test_bike_correct`;

#############导入基于地理位置，来自train表中的所有出发地和到达地正向关系的预测结果#############################
#############第一列预测概率	0.110520703529553
#############第一列覆盖概率	0.991697936
#############导入final_probability_trainstart_trainend_nearby_cross_pro表#################################
INSERT INTO probability_mix_4_temp(
	`orderid`,
	`geohashed_end_loc`,
	`geohashed_end_probability`,
	`geohashed_from`
)SELECT
	`orderid`,
	`geohashed_end_loc1`,
	`geohashed_end_probability1`,
	'final_probability_trainstart_trainend_nearby_cross_pro'
FROM
	`final_probability_trainstart_trainend_nearby_cross_pro`;

INSERT INTO probability_mix_4_temp(
	`orderid`,
	`geohashed_end_loc`,
	`geohashed_end_probability`,
	`geohashed_from`
)SELECT
	`orderid`,
	`geohashed_end_loc2`,
	`geohashed_end_probability2`,
	'final_probability_trainstart_trainend_nearby_cross_pro'
FROM
	`final_probability_trainstart_trainend_nearby_cross_pro`;

INSERT INTO probability_mix_4_temp(
	`orderid`,
	`geohashed_end_loc`,
	`geohashed_end_probability`,
	`geohashed_from`
)SELECT
	`orderid`,
	`geohashed_end_loc3`,
	`geohashed_end_probability3`,
	'final_probability_trainstart_trainend_nearby_cross_pro'
FROM
	`final_probability_trainstart_trainend_nearby_cross_pro`;
#############导入基于地理位置，来自test表中的所有出发地到达2公里内train所有到达地的预测结果#####################
#############第一列预测概率	0.0546270358912343
#############第一列覆盖概率	0.99999351
#############导入final_probability_teststart_trainend表###################################################
INSERT INTO probability_mix_4_temp(
	`orderid`,
	`geohashed_end_loc`,
	`geohashed_end_probability`,
	`geohashed_from`
)SELECT
	`orderid`,
	`geohashed_end_loc1`,
	`geohashed_end_probability1`,
	'final_probability_teststart_trainend'
FROM
	`final_probability_teststart_trainend`;

ALTER TABLE `probability_mix_4_temp` ADD INDEX geohashed_end_probability(`geohashed_end_probability`);

################导入数据完成###########################################

################生成预测概率表#########################################
DROP TABLE
IF EXISTS `probability_mix_4_order`;

CREATE TABLE `probability_mix_4_order` SELECT
*
FROM
	`probability_mix_4_temp`
ORDER BY
	`geohashed_end_probability` DESC;

ALTER TABLE `probability_mix_4_order` ADD PRIMARY KEY(`finalid`);
ALTER TABLE `probability_mix_4_order` ADD INDEX orderid(`orderid`);
ALTER TABLE `probability_mix_4_order` ADD INDEX geohashed_end_loc(`geohashed_end_loc`);

######################生成预测概率的备份表##################################################################
DROP TABLE
IF EXISTS `probability_mix_4_order_back`;

CREATE TABLE probability_mix_4_order_back SELECT
	*
FROM
	`probability_mix_4_order`;

ALTER TABLE `probability_mix_4_order_back` ADD PRIMARY KEY(`finalid`);

######################读取每个起点到某目标点最高概率值##########
DROP TABLE
IF EXISTS `test_userid_startandend_probability_top1`;

CREATE TABLE `test_userid_startandend_probability_top1` SELECT
	*
FROM
	`probability_mix_4_order`
GROUP BY
	orderid;

ALTER TABLE `test_userid_startandend_probability_top1` ADD PRIMARY KEY(`finalid`);
ALTER TABLE `test_userid_startandend_probability_top1` ADD INDEX orderid(`orderid`);
ALTER TABLE `test_userid_startandend_probability_top1` ADD INDEX geohashed_end_loc(`geohashed_end_loc`);

DELETE
FROM
	`probability_mix_4_order`
WHERE
	(
		`orderid`,
		`geohashed_end_loc`
	)IN(
		SELECT
			`orderid`,
			`geohashed_end_loc`
		FROM
			`test_userid_startandend_probability_top1`
	);

######################读取每个起点到某目标点次高概率值##########
DROP TABLE
IF EXISTS `test_userid_startandend_probability_top2`;

CREATE TABLE `test_userid_startandend_probability_top2` SELECT
	*
FROM
	`probability_mix_4_order`
GROUP BY
	orderid;

ALTER TABLE `test_userid_startandend_probability_top2` ADD PRIMARY KEY(`finalid`);
ALTER TABLE `test_userid_startandend_probability_top2` ADD INDEX orderid(`orderid`);
ALTER TABLE `test_userid_startandend_probability_top2` ADD INDEX geohashed_end_loc(`geohashed_end_loc`);

DELETE
FROM
	`probability_mix_4_order`
WHERE
	(
		`orderid`,
		`geohashed_end_loc`
	)IN(
		SELECT
			`orderid`,
			`geohashed_end_loc`
		FROM
			`test_userid_startandend_probability_top2`
	);


######################读取每个起点到某目标点第三高概率值########
DROP TABLE
IF EXISTS `test_userid_startandend_probability_top3`;

CREATE TABLE `test_userid_startandend_probability_top3` SELECT
	*
FROM
	`probability_mix_4_order`
GROUP BY
	orderid;

ALTER TABLE `test_userid_startandend_probability_top3` ADD PRIMARY KEY(`finalid`);
ALTER TABLE `test_userid_startandend_probability_top3` ADD INDEX orderid(`orderid`);
ALTER TABLE `test_userid_startandend_probability_top3` ADD INDEX geohashed_end_loc(`geohashed_end_loc`);

DROP TABLE
IF EXISTS `probability_mix_4_order`;

######################生成预测结果，带概率值##########################
DROP TABLE
IF EXISTS `final_probability_mix_4`;

CREATE TABLE final_probability_mix_4 SELECT
	`test`.`orderid` AS `orderid`,
	`test_userid_startandend_probability_top1`.`geohashed_end_loc` AS `geohashed_end_loc1`,
	`test_userid_startandend_probability_top1`.`geohashed_end_probability` AS `geohashed_end_probability1`,
	`test_userid_startandend_probability_top1`.`geohashed_from` AS `geohashed_from1`,
	`test_userid_startandend_probability_top2`.`geohashed_end_loc` AS `geohashed_end_loc2`,
	`test_userid_startandend_probability_top2`.`geohashed_end_probability` AS `geohashed_end_probability2`,
	`test_userid_startandend_probability_top2`.`geohashed_from` AS `geohashed_from2`,
	`test_userid_startandend_probability_top3`.`geohashed_end_loc` AS `geohashed_end_loc3`,
	`test_userid_startandend_probability_top3`.`geohashed_end_probability` AS `geohashed_end_probability3`,
	`test_userid_startandend_probability_top3`.`geohashed_from` AS `geohashed_from3`
FROM
	(
		(
			(
				`test`
				LEFT JOIN `test_userid_startandend_probability_top1` ON(
					(
						`test_userid_startandend_probability_top1`.`orderid` = `test`.`orderid`
					)
				)
			)
			LEFT JOIN `test_userid_startandend_probability_top2` ON(
				(
					`test_userid_startandend_probability_top2`.`orderid` = `test`.`orderid`
				)
			)
		)
		LEFT JOIN `test_userid_startandend_probability_top3` ON(
			(
				`test_userid_startandend_probability_top3`.`orderid` = `test`.`orderid`
			)
		)
	);

######################生成结果，缺失值随机填充wx4sqpb########
DROP TABLE
IF EXISTS `final_mix_4`;

CREATE TABLE final_mix_4 SELECT
	`test`.`orderid` AS `orderid`,
	`test_userid_startandend_probability_top1`.`geohashed_end_loc` AS `geohashed_end_loc1`,
	`test_userid_startandend_probability_top2`.`geohashed_end_loc` AS `geohashed_end_loc2`,
	`test_userid_startandend_probability_top3`.`geohashed_end_loc` AS `geohashed_end_loc3`
FROM
	(
		(
			(
				`test`
				LEFT JOIN `test_userid_startandend_probability_top1` ON(
					(
						`test_userid_startandend_probability_top1`.`orderid` = `test`.`orderid`
					)
				)
			)
			LEFT JOIN `test_userid_startandend_probability_top2` ON(
				(
					`test_userid_startandend_probability_top2`.`orderid` = `test`.`orderid`
				)
			)
		)
		LEFT JOIN `test_userid_startandend_probability_top3` ON(
			(
				`test_userid_startandend_probability_top3`.`orderid` = `test`.`orderid`
			)
		)
	);

UPDATE final_mix_4
SET geohashed_end_loc1 = 'wx4sqpb'
WHERE
	ISNULL(geohashed_end_loc1);

UPDATE final_mix_4
SET geohashed_end_loc2 = 'wx4sqpb'
WHERE
	ISNULL(geohashed_end_loc2);

UPDATE final_mix_4
SET geohashed_end_loc3 = 'wx4sqpb'
WHERE
	ISNULL(geohashed_end_loc3);

DROP TABLE test_userid_startandend_probability_top1;

DROP TABLE test_userid_startandend_probability_top2;

DROP TABLE test_userid_startandend_probability_top3;

DROP TABLE probability_mix_4_temp;