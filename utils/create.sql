CREATE TABLE IF NOT EXISTS `record`(
   `id` INT UNSIGNED AUTO_INCREMENT,
   `user_id` int NOT NULL,
   `_year` VARCHAR(40) NOT NULL,
   `_date` VARCHAR(40) NOT NULL,
  `amount` float NOT NULL,
   PRIMARY KEY ( `id` )
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `account`(
   `u_id` INT UNSIGNED AUTO_INCREMENT,
   `gender` VARCHAR(40) NOT NULL,
   `age` int NOT NULL,
  `address` VARCHAR(40),
   PRIMARY KEY ( `u_id` )
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

insert into account (u_id, gender, age, address) values (1, '男', 30, '浙江');
insert into account (u_id, gender, age, address) values (2, '女', 35, '北京');
insert into account (u_id, gender, age, address) values (3, '男', 15, '深圳');
insert into account (u_id, gender, age, address) values (4, '女', 20, '上海');


insert into record (user_id, _year, _date, amount) values (1, '2020', '01-28', 100);
insert into record (user_id, _year, _date, amount) values (1, '2021', '02-28', 200);
insert into record (user_id, _year, _date, amount) values (1, '2021', '06-1', 100);
insert into record (user_id, _year, _date, amount) values (2, '2021', '07-02', 200);
insert into record (user_id, _year, _date, amount) values (2, '2021', '07-22', 300);
insert into record (user_id, _year, _date, amount) values (2, '2021', '07-28', 200);
insert into record (user_id, _year, _date, amount) values (3, '2021', '08-28', 400);
insert into record (user_id, _year, _date, amount) values (4, '2021', '08-12', 500);


# docker exec -it 611187d7a4d8 env LANG=C.UTF-8 /bin/bash